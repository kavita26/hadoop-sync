package com.citusdata.sync.hdfs;

import java.net.InetSocketAddress;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.log4j.Logger;


public class CitusMasterNode {

	private static final Logger logger = Logger.getLogger(CitusMasterNode.class);

	private Connection masterNodeConnection = null;

	/* connection string format used in connecting to master node */
	private static final String CONNECTION_STRING_FORMAT =
		"jdbc:postgresql://%s:%d/postgres";

	/* remote function calls that fetch table and shard related metadata */
	private static final String MASTER_FETCH_TABLE_SHARD_IDS =
		"SELECT shardid FROM pg_dist_shard WHERE logicalrelid = ?";
	private static final String MASTER_FETCH_TABLE_SHARD_PLACEMENTS =
		"SELECT pg_dist_shard.shardid, shardlength, nodename" +
		" FROM pg_dist_shard, pg_dist_shard_placement" +
		" WHERE logicalrelid = ? AND" +
		" pg_dist_shard.shardid = pg_dist_shard_placement.shardid";
	private static final String MASTER_FETCH_TABLE_DDL_EVENTS =
		"SELECT * FROM master_get_table_ddl_events(?)";
	private static final String MASTER_FETCH_TABLE_METADATA =
		"SELECT * FROM master_get_table_metadata(?)";

	/* remote function calls that update shard and shard placement info */
	private static final String MASTER_INSERT_SHARD_ROW =
		"INSERT INTO pg_dist_shard" + 
		" (logicalrelid, shardid, shardstorage, shardminvalue, shardmaxvalue)" +
		" VALUES (?, ?, ?, ?, ?)";
	private static final String MASTER_DELETE_SHARD_ROW =
		"DELETE FROM pg_dist_shard WHERE shardid = ?";
	private static final String MASTER_INSERT_SHARD_PLACEMENT_ROW =
		"INSERT INTO pg_dist_shard_placement" +
		" (shardid, shardstate, shardlength, nodename, nodeport)" +
		" VALUES (?, ?, ?, ?, ?)";
	private static final String MASTER_DELETE_SHARD_PLACEMENT_ROW =
		"DELETE FROM pg_dist_shard_placement WHERE shardid = ? AND nodename = ?";

	/* remote function calls that fetch foreign table options */
	private static final String FETCH_FOREIGN_TABLE_OPTIONS =
		"SELECT ftoptions FROM pg_foreign_table  WHERE ftrelid = ?";
	private static final String FETCH_VALUE_FROM_OPTION_ARRAY =
		"SELECT option_value FROM pg_options_to_table(?) WHERE option_name = ?";

	/* column names used to identify response fields returned from master node */
	private static final String LOGICAL_RELID_FIELD = "logical_relid";
	private static final String SHARD_ID_FIELD = "shardid";
	private static final String SHARD_LENGTH_FIELD = "shardlength";
	private static final String SHARD_STORAGE_TYPE = "f";
	private static final String NODE_NAME_FIELD = "nodename";
	private static final String PART_KEY_FIELD = "part_key";
	private static final String FOREIGN_TABLE_OPTIONS_FIELD = "ftoptions";
	private static final String OPTION_VALUE_FIELD = "option_value";
	private static final int FILE_FINALIZED = 1;

	/*
	 * CitusMasterNode creates a JDBC connection to the CitusDB master database.
	 */
	public CitusMasterNode(String nodeName, int nodePort) throws SQLException {
		String connectionString = String.format(CONNECTION_STRING_FORMAT,
																						nodeName, nodePort);

		masterNodeConnection = DriverManager.getConnection(connectionString);
	}

	/*
	 * close closes the connection to the CitusDB master database.
	 */
	public void close() {
		try {
			if (masterNodeConnection != null) {
				masterNodeConnection.close();
				masterNodeConnection = null;
			}
		} catch (SQLException sqlException) {
			String logMessage = "could not close master node database connection";
			logger.warn(logMessage, sqlException);
		}
	}

	/*
	 * beginTransactionBlock begins a new transaction block.
	 */
	public void beginTransactionBlock() throws SQLException {
		masterNodeConnection.setAutoCommit(false);
	}

	/*
	 * commitTransactionBlock makes all changes in the started transaction block
	 * permanent.
	 */
	public void commitTransactionBlock() throws SQLException {
		masterNodeConnection.commit();
		masterNodeConnection.setAutoCommit(true);
	}

	/*
	 * fetchShardIdSet takes in the given distributed table name, and finds all
	 * shardIds that belong to this table.
	 */
	public Set<Long> fetchShardIdSet(String tableName) throws SQLException {
		Set<Long> shardIdSet = new HashSet<Long>();
		PreparedStatement fetchShardIdStatement = null;

		try {
			/* resolve this table's internal tableId */
			long tableId = fetchTableId(tableName);

			fetchShardIdStatement =
				masterNodeConnection.prepareStatement(MASTER_FETCH_TABLE_SHARD_IDS);
			fetchShardIdStatement.setLong(1, tableId);

			ResultSet shardIdResultSet = fetchShardIdStatement.executeQuery();

			while (shardIdResultSet.next()) {
				Long shardId = shardIdResultSet.getLong(SHARD_ID_FIELD);
				shardIdSet.add(shardId);
			}
		} finally {
			releaseResources(fetchShardIdStatement);
		}

		return shardIdSet;
	}

	/*
	 * fetchShardPlacementSet takes in the given distributed table name, and finds
	 * all shard placements (including replicas) that belong to this table.
	 */
	public SortedSet<ShardPlacement> fetchShardPlacementSet(String tableName)
		throws SQLException {
		SortedSet<ShardPlacement> shardPlacementSet = new TreeSet<ShardPlacement>();
		PreparedStatement fetchShardPlacementStatement = null;

		try {
			/* resolve this table's internal tableId */
			long tableId = fetchTableId(tableName);

			fetchShardPlacementStatement =
				masterNodeConnection.prepareStatement(MASTER_FETCH_TABLE_SHARD_PLACEMENTS);
			fetchShardPlacementStatement.setLong(1, tableId);

			ResultSet shardPlacementResultSet = fetchShardPlacementStatement.executeQuery();

			while (shardPlacementResultSet.next()) {
				long shardId = shardPlacementResultSet.getLong(SHARD_ID_FIELD);
				long shardLength = shardPlacementResultSet.getLong(SHARD_LENGTH_FIELD);
				String nodeName = shardPlacementResultSet.getString(NODE_NAME_FIELD);

				ShardPlacement shardPlacement = new ShardPlacement(shardId, shardLength, nodeName);
				shardPlacementSet.add(shardPlacement);
			}
		} finally {
			releaseResources(fetchShardPlacementStatement);
		}

		return shardPlacementSet;
	}

	/*
	 * fetchTableDDLEvents takes in the given distributed table name, and finds
	 * all DDL events needed to recreate a local version of the table.
	 */
	public List<String> fetchTableDDLEvents(String tableName) throws SQLException {
		List<String> tableDDLEventList = new LinkedList<String>();
		PreparedStatement fetchTableDDLStatement = null;

		try {
			fetchTableDDLStatement =
				masterNodeConnection.prepareStatement(MASTER_FETCH_TABLE_DDL_EVENTS);
			fetchTableDDLStatement.setString(1, tableName);

			ResultSet tableDDLEventResultSet = fetchTableDDLStatement.executeQuery();

			while (tableDDLEventResultSet.next()) {
				String tableDDLEvent = tableDDLEventResultSet.getString(1);
				tableDDLEventList.add(tableDDLEvent);
			}
		} finally {
			releaseResources(fetchTableDDLStatement);
		}

		return tableDDLEventList;
	}

	/*
	 * insertShardRow inserts tuple for a new logical shard into the pg_dist_shard
	 * system catalog.
	 */
	public void insertShardRow(String tableName, long shardId,
														 String minValue, String maxValue) throws SQLException {
		PreparedStatement insertShardRowStatement = null;

		try {
			/* resolve this table's internal tableId */
			long tableId = fetchTableId(tableName);

			insertShardRowStatement =
				masterNodeConnection.prepareStatement(MASTER_INSERT_SHARD_ROW);
			insertShardRowStatement.setLong(1, tableId);
			insertShardRowStatement.setLong(2, shardId);
			insertShardRowStatement.setString(3, SHARD_STORAGE_TYPE);
			insertShardRowStatement.setString(4, minValue);
			insertShardRowStatement.setString(5, maxValue);

			insertShardRowStatement.executeUpdate();

		} finally {
			releaseResources(insertShardRowStatement);
		}
	}

	/*
	 * deleteShardRow deletes tuple for a logical shard from the pg_dist_shard
	 * system catalog.
	 */
	public void deleteShardRow(long shardId) throws SQLException {
		PreparedStatement deleteShardRowStatement = null;

		try {
			deleteShardRowStatement =
				masterNodeConnection.prepareStatement(MASTER_DELETE_SHARD_ROW);
			deleteShardRowStatement.setLong(1, shardId);

			deleteShardRowStatement.executeUpdate();

		} finally {
			releaseResources(deleteShardRowStatement);
		}
	}

	/*
	 * insertShardPlacementRow inserts tuple for a new logical shard placement
	 * into the pg_dist_shard_placement system catalog.
	 */
	public void insertShardPlacementRow(ShardPlacement shardPlacement,
																			int shardNodePort) throws SQLException {
		PreparedStatement insertPlacementRowStatement = null;

		try {
			insertPlacementRowStatement =
					masterNodeConnection.prepareStatement(MASTER_INSERT_SHARD_PLACEMENT_ROW);
			insertPlacementRowStatement.setLong(1, shardPlacement.getShardId());
			insertPlacementRowStatement.setInt(2, FILE_FINALIZED);
			insertPlacementRowStatement.setLong(3, shardPlacement.getShardLength());
			insertPlacementRowStatement.setString(4, shardPlacement.getHostname());
			insertPlacementRowStatement.setInt(5, shardNodePort);

			insertPlacementRowStatement.executeUpdate();

		} finally {
			releaseResources(insertPlacementRowStatement);
		}
	}

	/*
	 * deleteShardPlacementRow deletes tuple for a logical shard placement from
	 * the pg_dist_shard_placement system catalog.
	 */
	public void deleteShardPlacementRow(ShardPlacement shardPlacement)
		throws SQLException {
		PreparedStatement deletePlacementRowStatement = null;

		try {
			deletePlacementRowStatement =
				masterNodeConnection.prepareStatement(MASTER_DELETE_SHARD_PLACEMENT_ROW);
			deletePlacementRowStatement.setLong(1, shardPlacement.getShardId());
			deletePlacementRowStatement.setString(2, shardPlacement.getHostname());

			deletePlacementRowStatement.executeUpdate();

		} finally {
			releaseResources(deletePlacementRowStatement);
		}
	}

	/*
	 * fetchPartitionColumn resolves the partition column of the given distributed
	 * table.
	 */
	public String fetchPartitionColumn(String tableName) throws SQLException {
		String partitionColumn = null;
		PreparedStatement fetchTableMetadataStatement = null;

		try {
			fetchTableMetadataStatement =
				masterNodeConnection.prepareStatement(MASTER_FETCH_TABLE_METADATA);
			fetchTableMetadataStatement.setString(1, tableName);

			ResultSet tableMetadataResultSet = fetchTableMetadataStatement.executeQuery();

			tableMetadataResultSet.next();
			partitionColumn = tableMetadataResultSet.getString(PART_KEY_FIELD);

		} finally {
			releaseResources(fetchTableMetadataStatement);
		}

		return partitionColumn;
	}

	/*
	 * fetchForeignTableOptionValue first fetches all option name/value pairs for
	 * the given foreign table name. The function then finds the option value that
	 * corresponds to the given option name, and returns this value.
	 */
	public String fetchForeignTableOptionValue(String tableName, String optionName)
		throws SQLException {
		String optionValue = null;
		PreparedStatement fetchTableOptionStatement = null;
		PreparedStatement fetchOptionValueStatement = null;

		try {
			/* resolve this table's internal tableId */
			long tableId = fetchTableId(tableName);

			/* first fetch foreign table option definition array */
			fetchTableOptionStatement =
				masterNodeConnection.prepareStatement(FETCH_FOREIGN_TABLE_OPTIONS);
			fetchTableOptionStatement.setLong(1, tableId);

			ResultSet tableOptionResultSet = fetchTableOptionStatement.executeQuery();
			boolean nextTableOption = tableOptionResultSet.next();
			if (!nextTableOption) {
				throw new SQLException("Could not find options for foreign table: " + tableName);
			}

			Array optionArray = tableOptionResultSet.getArray(FOREIGN_TABLE_OPTIONS_FIELD);

			/* now convert option array and find corresponding option value */
			fetchOptionValueStatement =
				masterNodeConnection.prepareStatement(FETCH_VALUE_FROM_OPTION_ARRAY);
			fetchOptionValueStatement.setArray(1, optionArray);
			fetchOptionValueStatement.setString(2, optionName);

			ResultSet optionValueResultSet = fetchOptionValueStatement.executeQuery();
			boolean nextOptionValue = optionValueResultSet.next();
			if (!nextOptionValue) {
				throw new SQLException("Could not find value for option name: " + optionName);
			}

			optionValue = optionValueResultSet.getString(OPTION_VALUE_FIELD);

		} finally {
			releaseResources(fetchTableOptionStatement);
			releaseResources(fetchOptionValueStatement);
		}

		return optionValue;
	}

	/*
	 * fetchTableId resolves the internal tableId used by CitusDB to identify the
	 * given distributed table.
	 */
	private long fetchTableId(String tableName) throws SQLException {
		long tableId = 0;
		PreparedStatement fetchTableMetadataStatement = null;

		try {
			fetchTableMetadataStatement =
				masterNodeConnection.prepareStatement(MASTER_FETCH_TABLE_METADATA);
			fetchTableMetadataStatement.setString(1, tableName);

			ResultSet tableMetadataResultSet = fetchTableMetadataStatement.executeQuery();

			tableMetadataResultSet.next();
			tableId = tableMetadataResultSet.getLong(LOGICAL_RELID_FIELD);

		} finally {
			releaseResources(fetchTableMetadataStatement);
		}

		return tableId;
	}

	/*
	 * releaseResources releases database and JDBC resources for the given statement.
	 */
	private void releaseResources(Statement statement) {
		try {
			if (statement != null) {
				statement.close();
			} 
		} catch (SQLException sqlException) {
			String logMessage = "could not release master node database resources";
			logger.warn(logMessage, sqlException);
		}
	}
}

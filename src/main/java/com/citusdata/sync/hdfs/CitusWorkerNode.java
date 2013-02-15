package com.citusdata.sync.hdfs;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.log4j.Logger;


public class CitusWorkerNode {

	private static final Logger logger = Logger.getLogger(CitusWorkerNode.class);
	
	private Connection workerNodeConnection = null;

	/* connection string format used in connecting to worker node */
	private static final String CONNECTION_STRING_FORMAT =
		"jdbc:postgresql://%s:%d/postgres";

	/* remote function calls to execute on worker node */
	private static final String SHARD_MIN_MAX_COMMAND =
		"SELECT min(%s), max(%s) FROM %s";
	private static final String APPLY_SHARD_DDL_COMMAND =
		"SELECT * FROM worker_apply_shard_ddl_command (?, ?)";
	private static final String SET_FOREIGN_TABLE_FILENAME =
		"ALTER FOREIGN TABLE %s OPTIONS (SET filename '%s')";
	private static final String DROP_FOREIGN_FILE_SERVER =
		"DROP SERVER IF EXISTS file_server CASCADE";

	/* internal definitions used to construct shard table name */
	private static final String SHARD_NAME_SEPARATOR = "_";
	private static final BigInteger TWO_TO_THE_POWER_OF_64 = BigInteger.ONE.shiftLeft(64);

	/*
	 * CitusWorkerNode creates a JDBC connection to the CitusDB worker database.
	 */
	public CitusWorkerNode(String nodeName, int nodePort) throws SQLException {
		String connectionString = String.format(CONNECTION_STRING_FORMAT,
																						nodeName, nodePort);

		workerNodeConnection = DriverManager.getConnection(connectionString);
	}

	/*
	 * close closes the connection to the CitusDB worker database.
	 */
	public void close() {
		try {
			if (workerNodeConnection != null) {
				workerNodeConnection.close();
				workerNodeConnection = null;
			}
		} catch (SQLException sqlException) {
			String logMessage = "could not close worker node database connection";
			logger.warn(logMessage, sqlException);
		}
	}

	/*
	 * createForeignTable takes the list of DDL commands needed to create the
	 * foreign table, and then applies them on the worker database. The function
	 * then sets the local file path for the newly created table.
	 */
	public void createForeignTable(long shardId, List<String> tableDDLEventList,
																 String tableName, String localFilePath)
		throws SQLException {
		PreparedStatement ddlEventStatement = null;
		PreparedStatement foreignFileStatement = null;

		try {
			/* send all statements within one transaction block */
			workerNodeConnection.setAutoCommit(false);

			/* execute ddl events needed to create the table */
			for (String tableDDLEvent : tableDDLEventList) {
				ddlEventStatement =
					workerNodeConnection.prepareStatement(APPLY_SHARD_DDL_COMMAND);
				ddlEventStatement.setLong(1, shardId);
				ddlEventStatement.setString(2, tableDDLEvent);

				ddlEventStatement.execute();
			}

			/* set foreign table's local file path */
			String foreignFileString = String.format(SET_FOREIGN_TABLE_FILENAME,
																							 tableName, localFilePath);
			foreignFileStatement =
				workerNodeConnection.prepareStatement(APPLY_SHARD_DDL_COMMAND);
			foreignFileStatement.setLong(1, shardId);
			foreignFileStatement.setString(2, foreignFileString);

			foreignFileStatement.execute();

			/* commit all statements */
			workerNodeConnection.commit();

		} catch (SQLException sqlException) {
			/* rollback may also trigger an exception */
			workerNodeConnection.rollback();
			throw sqlException;

		} finally {
			releaseResources(ddlEventStatement);
			releaseResources(foreignFileStatement);

			workerNodeConnection.setAutoCommit(true);
		}
	}

	/*
	 * dropForeignFileServer removes the foreign file server and foreign table for
	 * the given shardId, if such a foreign file server exists.
	 */
	public void dropForeignFileServer(long shardId) throws SQLException {
		PreparedStatement dropStatement = null;
		try {
			dropStatement = workerNodeConnection.prepareStatement(APPLY_SHARD_DDL_COMMAND);
			dropStatement.setLong(1, shardId);
			dropStatement.setString(2, DROP_FOREIGN_FILE_SERVER);

			dropStatement.execute();
		} finally {
			releaseResources(dropStatement);
		}
	}

	/*
	 * fetchShardMinMaxValue fetches the partition column's min/max values from
	 * the remote shard.
	 */
	public MinMaxValue fetchShardMinMaxValue(long shardId, String tableName,
																					 String partitionColumn)
		throws SQLException {
		MinMaxValue minMaxValue = null;
		PreparedStatement shardMinMaxStatement = null;

		try {
			String shardTableName = getShardTableName(tableName, shardId);
			String shardMinMaxString = String.format(SHARD_MIN_MAX_COMMAND, partitionColumn,
																							 partitionColumn, shardTableName);

			shardMinMaxStatement = workerNodeConnection.prepareStatement(shardMinMaxString);

			ResultSet minMaxResultSet = shardMinMaxStatement.executeQuery();
			minMaxResultSet.next();

			String minValue = minMaxResultSet.getString(1);
			String maxValue = minMaxResultSet.getString(2);
			minMaxValue = new MinMaxValue(minValue, maxValue);

		} finally {
			releaseResources(shardMinMaxStatement);
		}

		return minMaxValue;
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
			String logMessage = "could not release worker node database resources";
			logger.warn(logMessage, sqlException);
		}
	}

	/*
	 * getShardTableName converts signed shardId to its unsigned representation,
	 * and then appends this unsigned shardId to the base table name.
	 */
	private String getShardTableName(String tableName, long shardId) {
		BigInteger unsignedShardId = BigInteger.valueOf(shardId);
		if (unsignedShardId.signum() < 0) {
			unsignedShardId = unsignedShardId.add(TWO_TO_THE_POWER_OF_64);
		}

		String shardTableName = tableName + SHARD_NAME_SEPARATOR + unsignedShardId.toString();
		return shardTableName;
	}
}

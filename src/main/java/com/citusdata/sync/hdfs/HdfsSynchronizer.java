package com.citusdata.sync.hdfs;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;


public class HdfsSynchronizer {

  private static final Logger logger = Logger.getLogger(HdfsSynchronizer.class);
  private static Configuration hdfsConfig = new Configuration();

  /* command line flag to fetch min/max values */
  private static final String FETCH_MIN_MAX_OPTION = "--fetch-min-max";

  /*
   * We synchronize metadata for all blocks that reside in an HDFS directory. We
   * get that directory's name from the distributed foreign table created on the
   * CitusDB master. The directory's name is specified as a foreign table option.
   */
  private static final String HDFS_DIRECTORY_OPTION_NAME = "hdfs_directory_path";

  /* HDFS node name and port numbers */
  private final String hdfsMasterNodeName;
  private final int hdfsMasterNodePort;
  private final int hdfsWorkerNodePort;

  /* CitusDB node name and port numbers */
  private final String citusMasterNodeName;
  private final int citusMasterNodePort;
  private final int citusWorkerNodePort;

  /*
   * main reads and validates command line arguments, and then creates an HDFS
   * synchronizer to propagate HDFS table metadata to the CitusDB cluster.
   */
  public static void main(String[] args) {
    if (args.length < 1) {
      logger.error("usage: HdfsSynchronizer table_name [--fetch-min-max]");
      System.exit(-1);
    } else if (args.length >= 2) {
      if (!args[1].equals(FETCH_MIN_MAX_OPTION)) {
        logger.error("usage: HdfsSynchronizer table_name [--fetch-min-max]");
        System.exit(-1);
      }
    }

    String tableName = args[0];
    boolean fetchMinMaxValue = false;
    if (args.length >= 2) {
      fetchMinMaxValue = true;
    }

    try {
      HdfsSynchronizer hdfsSynchronizer = new HdfsSynchronizer();
      MetadataDifference metadata =
        hdfsSynchronizer.calculateMetadataDifference(tableName);
      hdfsSynchronizer.syncMetadataDifference(tableName, metadata, fetchMinMaxValue);

      logger.info("synchronized metadata for table: " + tableName);

    } catch (IOException ioException) {
      logger.error("could not synchronize table metadata", ioException);
    } catch (SQLException sqlException) {
      logger.error("could not synchronize table metadata", sqlException);
    }
  }

  /*
   * HdfsSynchronizer creates a new object that synchronizes an HDFS directory's
   * metadata to a CitusDB table. This class leaves the CitusDB metadata in a
   * consistent state and expects the user to re-run the application in case one
   * of the following happens: (a) we lose the connection to the CitusDB master
   * node, (b) all block locations for an HDFS block are unreachable, or (c) we
   * fail creating foreign tables for all replicas of an HDFS block on workers.
   */
  public HdfsSynchronizer() {
    CompositeConfiguration syncConfig = new CompositeConfiguration();

    try {
      syncConfig.addConfiguration(new SystemConfiguration());
      syncConfig.addConfiguration(new PropertiesConfiguration("sync.properties"));
    } catch (ConfigurationException exception) {
      logger.warn("could not load configuration file, using defaults", exception);
    }

    hdfsMasterNodeName = syncConfig.getString("HdfsMasterNodeName", "localhost");
    hdfsMasterNodePort = syncConfig.getInt("HdfsMasterNodePort", 9000);
    hdfsWorkerNodePort = syncConfig.getInt("HdfsWorkerNodePort", 50020);

    citusMasterNodeName = syncConfig.getString("CitusMasterNodeName", "localhost");
    citusMasterNodePort = syncConfig.getInt("CitusMasterNodePort", 5432);
    citusWorkerNodePort = syncConfig.getInt("CitusWorkerNodePort", 9700);
  }

  /*
   * MetadataDifference acts as an immutable value class that keeps shard and
   * shard placement metadata differences between the CitusDB and HDFS master
   * nodes. New shard and shard placements are those that appear in the HDFS
   * master node and that need to be inserted into CitusDB. Old shard and shard
   * placements are the opposite; these have been removed from HDFS and should
   * now be dropped from CitusDB.
   */
  private static class MetadataDifference {
    public final Set<Long> newShardIdSet;
    public final Set<Long> oldShardIdSet;
    public final Set<ShardPlacement> citusShardPlacementSet;
    public final Set<ShardPlacement> newShardPlacementSet;
    public final Set<ShardPlacement> oldShardPlacementSet;

    public MetadataDifference(Set<Long> newShardIdSet, Set<Long> oldShardIdSet,
                              Set<ShardPlacement> citusShardPlacementSet,
                              Set<ShardPlacement> newShardPlacementSet,
                              Set<ShardPlacement> oldShardPlacementSet) {
      this.newShardIdSet = newShardIdSet;
      this.oldShardIdSet = oldShardIdSet;
      this.citusShardPlacementSet = citusShardPlacementSet;
      this.newShardPlacementSet = newShardPlacementSet;
      this.oldShardPlacementSet = oldShardPlacementSet;
    }
  }

  /*
   * calculateMetadataDifference computes metadata differences between the HDFS
   * and the CitusDB master node. This means finding shardId (blockId) and shard
   * placement (block location) metadata that has been removed and added to HDFS
   * and returning these differences in a value object.
   */
  private MetadataDifference calculateMetadataDifference(String tableName)
    throws IOException, SQLException {
    MetadataDifference metadataDifference = null;
    HdfsMasterNode hdfsMasterNode = null;
    CitusMasterNode citusMasterNode = null;

    try {
      /* connect to HDFS and CitusDB master nodes */
      hdfsMasterNode = new HdfsMasterNode(hdfsMasterNodeName, hdfsMasterNodePort,
                                          hdfsConfig);
      citusMasterNode = new CitusMasterNode(citusMasterNodeName, citusMasterNodePort);

      /*
       * In our model, one distributed foreign table maps to an HDFS directory.
       * We keep that directory path as an option in the CitusDB foreign table,
       * and resolve the path from there.
       */
      String hdfsDirectoryPath = citusMasterNode.fetchForeignTableOptionValue(
        tableName, HDFS_DIRECTORY_OPTION_NAME);

      /* fetch shard and shard placement metadata for the HDFS directory */
      Set<Long> hdfsShardIdSet = hdfsMasterNode.fetchShardIdSet(hdfsDirectoryPath);
      SortedSet<ShardPlacement> hdfsShardPlacementSet =
        hdfsMasterNode.fetchShardPlacementSet(hdfsDirectoryPath);

      /* fetch shard and shard placement metadata for the CitusDB table */
      Set<Long> citusShardIdSet = citusMasterNode.fetchShardIdSet(tableName);
      SortedSet<ShardPlacement> citusShardPlacementSet =
        citusMasterNode.fetchShardPlacementSet(tableName);

      /* first, find old shard placements to remove from CitusDB */
      SortedSet<ShardPlacement> oldShardPlacementSet =
        new TreeSet<ShardPlacement>(citusShardPlacementSet);
      oldShardPlacementSet.removeAll(hdfsShardPlacementSet);

      /* second, find new shard placements to add to CitusDB */
      SortedSet<ShardPlacement> newShardPlacementSet =
        new TreeSet<ShardPlacement>(hdfsShardPlacementSet);
      newShardPlacementSet.removeAll(citusShardPlacementSet);

      /* third, find old shard ids to remove from CitusDB */
      Set<Long> oldShardIdSet = new HashSet<Long>(citusShardIdSet);
      oldShardIdSet.removeAll(hdfsShardIdSet);

      /* fourth, find new shard ids to add to CitusDB */
      Set<Long> newShardIdSet = new HashSet<Long>(hdfsShardIdSet);
      newShardIdSet.removeAll(citusShardIdSet);

      /* create value object that stores table metadata differences */
      metadataDifference = new MetadataDifference(newShardIdSet, oldShardIdSet,
                                                  citusShardPlacementSet,
                                                  newShardPlacementSet,
                                                  oldShardPlacementSet);
    } finally {
      if (hdfsMasterNode != null) {
        hdfsMasterNode.close();
      }
      if (citusMasterNode != null) {
        citusMasterNode.close();
      }
    }

    return metadataDifference;
  }

  /*
   * syncMetadataDifference updates metadata on the CitusDB cluster to resolve
   * any differences with the HDFS master node. For this, the function first
   * walks over shard placement differences, and drops/creates foreign tables
   * that correspond to shard placements in an idempotent manner. The function
   * then propagates successful shard placement updates to the CitusDB master
   * node. Last, the function resolves shardId differences on the master node.
   */
  private void syncMetadataDifference(String tableName, MetadataDifference metadata,
                                      boolean fetchMinMaxValue) throws SQLException {
    CitusMasterNode citusMasterNode = null;

    try {
      citusMasterNode = new CitusMasterNode(citusMasterNodeName, citusMasterNodePort);
      String foreignServerName = citusMasterNode.fetchForeignServerName(tableName);
      
      citusMasterNode.beginTransactionBlock();

      /* first, remove old shard placements from CitusDB and track removed shards */
      SortedSet<ShardPlacement> deletedShardPlacementSet = new TreeSet<ShardPlacement>();

      for (ShardPlacement oldShardPlacement : metadata.oldShardPlacementSet) {
        boolean dropped = dropShardPlacementTable(oldShardPlacement, foreignServerName);
        if (dropped) {
          citusMasterNode.deleteShardPlacementRow(oldShardPlacement);

          deletedShardPlacementSet.add(oldShardPlacement);
        }
      }

      /* second, add new shard placements to CitusDB and track added shards */
      List<String> tableDDLEventList = citusMasterNode.fetchTableDDLEvents(tableName);

      SortedSet<ShardPlacement> insertedShardPlacementSet = new TreeSet<ShardPlacement>();

      for (ShardPlacement newShardPlacement : metadata.newShardPlacementSet) {
        boolean created = createShardPlacementTable(newShardPlacement, foreignServerName,
                                                    tableName, tableDDLEventList);
        if (created) {
          int newShardNodePort = citusWorkerNodePort;
          citusMasterNode.insertShardPlacementRow(newShardPlacement, newShardNodePort);

          insertedShardPlacementSet.add(newShardPlacement);
        }
      }

      /* calculate current set of shard placements in CitusDB */
      SortedSet<ShardPlacement> currentShardPlacementSet =
        new TreeSet<ShardPlacement>(metadata.citusShardPlacementSet);
      currentShardPlacementSet.removeAll(deletedShardPlacementSet);
      currentShardPlacementSet.addAll(insertedShardPlacementSet);

      /*
       * Third, we remove old shards from CitusDB. We also check that we don't
       * have any shard placements corresponding to old shardIds.
       */
      for (Long oldShardId : metadata.oldShardIdSet) {
        Set<ShardPlacement> oldShardPlacementSet =
          shardIdPlacementSet(oldShardId, currentShardPlacementSet);

        if (!oldShardPlacementSet.isEmpty()) {
          throw new SQLException("Could not remove shard placements for shardId: " +
                                 oldShardId);
        }

        citusMasterNode.deleteShardRow(oldShardId);
      }

      /*
       * Fourth, we add new shardIds to CitusDB in a transaction block. We also
       * check that we have at least one shard placement corresponding to each
       * new new shardId. If we don't, we failed to synchronize a new HDFS block
       * and we error out.
       */

      for (Long newShardId : metadata.newShardIdSet) {
        Set<ShardPlacement> newShardPlacementSet =
          shardIdPlacementSet(newShardId, currentShardPlacementSet);

        if (newShardPlacementSet.isEmpty()) {
          throw new SQLException("Could not find shard placement for shardId: " +
                                 newShardId);
        }

        /* if user asked for min/max statistics, fetch and store them */
        if (fetchMinMaxValue) {
          String partitionColumn = citusMasterNode.fetchPartitionColumn(tableName);
          Iterator shardPlacementIter = newShardPlacementSet.iterator();
          MinMaxValue minMaxValue = null;

          while ((minMaxValue == null) && shardPlacementIter.hasNext()) {
            ShardPlacement shardPlacement = (ShardPlacement) shardPlacementIter.next();
            String workerNodeName = shardPlacement.getHostname();
            long shardId = shardPlacement.getShardId();

            minMaxValue = fetchShardMinMaxValue(workerNodeName, shardId,
                                                tableName, partitionColumn);
          }

          /* if failed to fetch min/max values from any node, error out */
          if (minMaxValue == null) {
            throw new SQLException("Could not fetch min/max values for shardId: " +
                                   newShardId);
          }

          String minValue = minMaxValue.getMinValue();
          String maxValue = minMaxValue.getMaxValue();

          citusMasterNode.insertShardRow(tableName, newShardId, minValue, maxValue);
        } else {
          citusMasterNode.insertShardRow(tableName, newShardId, null, null);
        }
      }

      citusMasterNode.commitTransactionBlock();

    } catch (SQLException sqlException) {
      citusMasterNode.rollbackTransactionBlock();
      throw sqlException;

    } finally {
      if (citusMasterNode != null) {
        citusMasterNode.close();
      }
    }
  }

  /*
   * dropShardPlacementTable finds the worker node the shard placement resides
   * on, and connects to that node. The function then issues command to drop the
   * foreign server/table that corresponds to the given shard placement.
   */
  private boolean dropShardPlacementTable(ShardPlacement shardPlacement, 
                                          String foreignServerName) {
    CitusWorkerNode citusWorkerNode = null;
    boolean tableDropped = true;

    try {
      String workerNodeName = shardPlacement.getHostname();
      long shardId = shardPlacement.getShardId();

      /* drop shard placement's foreign table on worker node */
      citusWorkerNode = new CitusWorkerNode(workerNodeName, citusWorkerNodePort);
      citusWorkerNode.dropForeignServer(shardId, foreignServerName);

    } catch (SQLException sqlException) {
      String logMessage = "could not drop shard placement from worker node";
      logger.warn(logMessage, sqlException);

      tableDropped = false;
    } finally {
      if (citusWorkerNode != null) {
        citusWorkerNode.close();
      }
    }

    return tableDropped;
  }

  /*
   * createShardPlacementTable finds the worker node to create the given shard
   * placement, and connects to that node. The function then resolves the shard
   * placement's local file path from the HDFS worker node, and then creates a
   * foreign table for the file. The function also tells the worker node to drop
   * any existing foreign table for this shard to ensure idempotent behavior.
   */
  private boolean createShardPlacementTable(ShardPlacement shardPlacement,
                                            String foreignServerName,
                                            String tableName,
                                            List<String> tableDDLEventList) {
    HdfsWorkerNode hdfsWorkerNode = null;
    CitusWorkerNode citusWorkerNode = null;
    boolean tableCreated = true;

    try {
      String workerNodeName = shardPlacement.getHostname();
      long shardId = shardPlacement.getShardId();
      long shardLength = shardPlacement.getShardLength();

      /* find shard placement's local file path from hdfs worker node */
      hdfsWorkerNode = new HdfsWorkerNode(workerNodeName, hdfsWorkerNodePort, hdfsConfig);
      String localFilePath = hdfsWorkerNode.fetchShardLocalFilePath(shardId, shardLength);

      citusWorkerNode = new CitusWorkerNode(workerNodeName, citusWorkerNodePort);
      citusWorkerNode.dropForeignServer(shardId, foreignServerName); /* drop if exists */
      citusWorkerNode.createForeignTable(shardId, tableDDLEventList,
                                         tableName, localFilePath);

    } catch (IOException ioException) {
      String logMessage = "could not create shard placement on worker node";
      logger.warn(logMessage, ioException);

      tableCreated = false;
    } catch (SQLException sqlException) {
      String logMessage = "could not create shard placement on worker node";
      logger.warn(logMessage, sqlException);

      tableCreated = false;
    } finally {
      if (hdfsWorkerNode != null) {
        hdfsWorkerNode.close();
      }
      if (citusWorkerNode != null) {
        citusWorkerNode.close();
      }
    }

    return tableCreated;
  }

  /*
   * shardIdPlacementSet looks for shard placements that correspond to the given
   * shardId, and returns the found shard placements in a set.
   */
  private Set<ShardPlacement> shardIdPlacementSet(
    long shardId, SortedSet<ShardPlacement> shardPlacementSet) {
    String emptyHostname = new String();
    ShardPlacement fromShardPlacement = new ShardPlacement(shardId, 0, emptyHostname);
    ShardPlacement toShardPlacement = new ShardPlacement((shardId + 1), 0, emptyHostname);

    Set<ShardPlacement> shardIdPlacementSet =
      shardPlacementSet.subSet(fromShardPlacement, toShardPlacement);

    return shardIdPlacementSet;
  }

  /*
   * fetchShardMinMaxValue connects to database at the given worker node, and
   * runs query to fetch the given shard's partition column's min/max values.
   * These values are later used by CitusDB for partition and join pruning.
   */
  private MinMaxValue fetchShardMinMaxValue(String workerNodeName, long shardId,
                                            String tableName, String partitionColumn) {
    MinMaxValue minMaxValue = null;
    CitusWorkerNode citusWorkerNode = null;

    try {
      citusWorkerNode = new CitusWorkerNode(workerNodeName, citusWorkerNodePort);

      minMaxValue = citusWorkerNode.fetchShardMinMaxValue(shardId, tableName,
                                                          partitionColumn);
    } catch(SQLException sqlException) {
      String logMessage = "could not fetch min/max values from worker node";
      logger.warn(logMessage, sqlException);

    } finally {
      if (citusWorkerNode != null) {
        citusWorkerNode.close();
      }
    }

    return minMaxValue;
  }
}

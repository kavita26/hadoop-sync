package com.citusdata.sync.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.RPC;


public class HdfsMasterNode {

	private ClientProtocol nameNodeClient = null;

	/*
	 * HdfsMasterNode creates a proxy connection to the HDFS namenode.
	 */
	public HdfsMasterNode(String nodeName, int nodePort, Configuration config)
		throws IOException {
		InetSocketAddress nodeAddress = new InetSocketAddress(nodeName, nodePort);

		nameNodeClient = (ClientProtocol) RPC.getProxy(ClientProtocol.class,
																									 ClientProtocol.versionID,
																									 nodeAddress, config);
	}

	/*
	 * close closes the proxy connection to the HDFS namenode.
	 */
	public void close() {
		if (nameNodeClient != null) {
			RPC.stopProxy(nameNodeClient);
			nameNodeClient = null;
		}
	}

	/*
	 * fetchShardIdSet takes the given HDFS directory path, and walks over HDFS
	 * files within that path. The function then finds all shards that make up
	 * these files, and returns the de-duplicated shardIds in a set.
	 */
	public Set<Long> fetchShardIdSet(String directoryPath)
		throws IOException {
		Set<Long> shardIdSet = new HashSet<Long>();

		/* find files in HDFS directory */
		List<String> filePathList = getFilePathList(directoryPath);
		List<LocatedBlock> locatedBlockList = getLocatedBlockList(filePathList);

		for (LocatedBlock locatedBlock : locatedBlockList) {
			Block block = locatedBlock.getBlock();
			Long blockId = Long.valueOf(block.getBlockId());

			if (!shardIdSet.contains(blockId)) {
				shardIdSet.add(blockId);
			}
		}

		return shardIdSet;
	}

	/*
	 * fetchShardPlacementSet takes the given HDFS directory path, and walks over
	 * HDFS files within that path. The function then finds all shards that make
	 * up these files, and returns the shard placements in a set.
	 */
	public SortedSet<ShardPlacement> fetchShardPlacementSet(String directoryPath)
		throws IOException {
		SortedSet<ShardPlacement> shardPlacementSet = new TreeSet<ShardPlacement>();

		/* find files in HDFS directory */
		List<String> filePathList = getFilePathList(directoryPath);
		List<LocatedBlock> locatedBlockList = getLocatedBlockList(filePathList);

		for (LocatedBlock locatedBlock : locatedBlockList) {
			Block block = locatedBlock.getBlock();
			long blockId = block.getBlockId();
			long blockLength = block.getNumBytes();

			DatanodeInfo[] dataNodeArray = locatedBlock.getLocations();
			for (DatanodeInfo dataNode : dataNodeArray) {
				String dataNodeName = dataNode.getHostName();

				/* create and add shard placement */
				ShardPlacement shardPlacement =
						new ShardPlacement(blockId, blockLength, dataNodeName);
				shardPlacementSet.add(shardPlacement);
			}
		}

		return shardPlacementSet;
	}

	/*
	 * getFilePathList recursively walks over the given directory path and finds
	 * all regular files within this path. The function then resolves full paths
	 * for the found files, and adds these file paths to a list.
	 */
	private List<String> getFilePathList(String directoryPath)
		throws IOException {
		List<String> filePathList = new LinkedList<String>();

		byte[] lastFilename = HdfsFileStatus.EMPTY_NAME;
		boolean hasMoreFiles = true;

		while (hasMoreFiles) {
			/* get a partial listing of files in current directory */
			DirectoryListing partialListing =
				nameNodeClient.getListing(directoryPath, lastFilename);

			if (partialListing == null) {
				throw new IllegalStateException("HDFS path does not exist: " + directoryPath);
			}

			HdfsFileStatus[] fileStatusArray = partialListing.getPartialListing();
			for (HdfsFileStatus fileStatus : fileStatusArray) {
				String filePath = fileStatus.getFullName(directoryPath);

				/* if we have a subdirectory, recurse into it */
				boolean isDirectory = fileStatus.isDir();
				if (isDirectory) {
					List<String> subDirectoryFilePathList = getFilePathList(filePath);
					filePathList.addAll(subDirectoryFilePathList);
				} else {
					filePathList.add(filePath);
				}

				/* move last filename iterator forward */
				lastFilename = fileStatus.getLocalNameInBytes();
			}

			hasMoreFiles = partialListing.hasMore();
		}

		return filePathList;
	}

	/*
	 * getLocatedBlockList walks over files in the given list. The function then
	 * finds the blocks that make up each file and appends these block locations
	 * to a list. The function next returns the list.
	 */
	private List<LocatedBlock> getLocatedBlockList(List<String> filePathList)
		throws IOException {
		List<LocatedBlock> locatedBlockList = new LinkedList<LocatedBlock>();

		for (String filePath : filePathList) {
			HdfsFileStatus fileStatus = nameNodeClient.getFileInfo(filePath);
			long fileStartOffset = 0;
			long fileLength = fileStatus.getLen();

			/* find all blocks that belong to this file */
			LocatedBlocks fileBlocks =
					nameNodeClient.getBlockLocations(filePath, fileStartOffset, fileLength);
			List<LocatedBlock> fileBlockList = fileBlocks.getLocatedBlocks();

			locatedBlockList.addAll(fileBlockList);
		}

		return locatedBlockList;
	}
}

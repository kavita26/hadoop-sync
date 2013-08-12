package com.citusdata.sync.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.Token;


public class HdfsWorkerNode {

  private ClientDatanodeProtocol dataNodeClient = null;

  /*
   * HdfsWorkerNode creates a proxy connection to the given HDFS datanode.
   */
  public HdfsWorkerNode(String nodeName, int nodePort, Configuration config)
    throws IOException {
    InetSocketAddress nodeAddress = new InetSocketAddress(nodeName, nodePort);

    dataNodeClient = (ClientDatanodeProtocol) RPC.getProxy(ClientDatanodeProtocol.class,
                                                           ClientDatanodeProtocol.versionID,
                                                           nodeAddress, config);
  }

  /*
   * close closes the proxy connection to the HDFS datanode.
   */
  public void close() {
    if (dataNodeClient != null) {
      RPC.stopProxy(dataNodeClient);
      dataNodeClient = null;
    }
  }

  /*
   * fetchShardLocalFilePath contacts the HDFS datanode, and resolves the given
   * shard's local file path on that node. The function then returns this path.
   */
  public String fetchShardLocalFilePath(long blockId, long blockLength)
    throws IOException {
    /* create block and token object to resolve local filesystem path */
    Block block = new Block(blockId, blockLength, GenerationStamp.WILDCARD_STAMP);
    Token<BlockTokenIdentifier> blockToken = new Token<BlockTokenIdentifier>();

    BlockLocalPathInfo localBlockPathInfo =
      dataNodeClient.getBlockLocalPathInfo(block, blockToken);

    String localFilePath = localBlockPathInfo.getBlockPath();
    return localFilePath;
  }
}

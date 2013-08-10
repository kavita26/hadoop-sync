package com.citusdata.sync.hdfs;


public class ShardPlacement implements Comparable {

  private final long shardId;
  private final long shardLength;
  private final String hostname;

  /*
   * ShardPlacement creates a new shard placement.
   */
  public ShardPlacement(long shardId, long shardLength, String hostname) {
    if (hostname == null) {
      throw new NullPointerException();
    }

    this.shardId = shardId;
    this.shardLength = shardLength;
    this.hostname = hostname;
  }

  /*
   * getShardId returns the shard placement's shardId.
   */
  public long getShardId() {
    return shardId;
  }

  /*
   * getShardLength returns the shard placement's shard length.
   */
  public long getShardLength() {
    return shardLength;
  }

  /*
   * getHostname returns the host name this shard placement lives on.
   */
  public String getHostname() {
    return hostname;
  }

  /*
   * equals checks if the given object equals to this shard placement.
   */
  @Override
  public boolean equals(Object object) {
    if (!(object instanceof ShardPlacement)) {
      return false;
    }

    ShardPlacement obj = (ShardPlacement) object;
    if (obj.shardId == shardId && obj.shardLength == shardLength &&
        obj.hostname.equals(hostname)) {
      return true;
    }

    return false;
  }

  /*
   * hashCode generates a hash code for this shard placement.
   */
  @Override
  public int hashCode() {
    int result = 17;
    result = (31 * result) + (int) (shardId ^ (shardId >>> 32));
    result = (31 * result) + (int) (shardLength ^ (shardLength >>> 32));
    result = (31 * result) + hostname.hashCode();
    return result;
  }

  /*
   * compareTo provides a natural ordering for shard placements.
   */
  @Override
  public int compareTo(Object object) {
    ShardPlacement obj = (ShardPlacement) object;

    /* first compare shard ids */
    if (shardId > obj.getShardId()) {
      return 1;
    } else if (shardId < obj.getShardId()) {
      return -1;
    }

    /* second compare shard lengths */
    if (shardLength > obj.getShardLength()) {
      return 1;
    } else if (shardLength < obj.getShardLength()) {
      return -1;
    }

    /* third compare hostnames */
    int hostnameDiff = hostname.compareTo(obj.getHostname());
    return hostnameDiff;
  }
}

package com.continuuity.common.zookeeper.election.internal;

import com.continuuity.common.zookeeper.election.Election;
import com.google.common.base.Objects;

/**
 * Represents an election that is registered with ZooKeeper.
 */
public class RegisteredElection extends Election {
  private final long seqId;
  private final String zkPath;

  public RegisteredElection(Election election, long seqId, String zkPath) {
    super(election);
    this.seqId = seqId;
    this.zkPath = zkPath;
  }

  public long getSeqId() {
    return seqId;
  }

  public String getZkPath() {
    return zkPath;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("election", super.toString())
      .add("seqId", seqId)
      .add("zkPath", zkPath)
      .toString();
  }
}

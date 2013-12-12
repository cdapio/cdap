/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase.coprocessor;

/**
 * Helper class for holding consumer groupId and instanceId for hash map lookup.
 *
 * NOTE: This class is not thread safe.
 */
public final class ConsumerInstance {
  private long groupId;
  private int instanceId;
  private boolean hashCodeComputed;
  private int hashCode;

  public ConsumerInstance(long groupId, int instanceId) {
    setGroupInstance(groupId, instanceId);
  }

  public void setGroupInstance(long groupId, int instanceId) {
    this.groupId = groupId;
    this.instanceId = instanceId;
    this.hashCodeComputed = false;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || o.getClass() != ConsumerInstance.class) {
      return false;
    }
    ConsumerInstance other = (ConsumerInstance) o;
    return groupId == other.groupId && instanceId == other.instanceId;
  }

  @Override
  public int hashCode() {
    if (hashCodeComputed) {
      return hashCode;
    }

    hashCode = (int) ((groupId ^ (groupId >> 32)) ^ instanceId);
    hashCodeComputed = true;
    return hashCode;
  }
}

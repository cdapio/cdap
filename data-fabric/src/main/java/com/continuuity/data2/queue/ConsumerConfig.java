/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 *
 */
public final class ConsumerConfig {

  private final long groupId;
  private final int instanceId;
  private final int groupSize;
  private final int numGroups;
  private final DequeueStrategy dequeueStrategy;
  private final String hashKey;

  public ConsumerConfig(long groupId, int instanceId, int groupSize, DequeueStrategy dequeueStrategy, String hashKey) {
    Preconditions.checkArgument(instanceId >= 0, "Instance ID must be >= 0.");
    Preconditions.checkArgument(instanceId < groupSize, "Instance ID must be < groupSize");
    this.groupId = groupId;
    this.instanceId = instanceId;
    this.groupSize = groupSize;
    this.dequeueStrategy = dequeueStrategy;
    this.hashKey = dequeueStrategy == DequeueStrategy.HASH ? hashKey : null;
    this.numGroups = 0;
  }

  public ConsumerConfig(long groupId, int instanceId, int groupSize, int numGroups, DequeueStrategy dequeueStrategy,
                        String hashKey) {
    Preconditions.checkArgument(instanceId >= 0, "Instance ID must be >= 0.");
    Preconditions.checkArgument(instanceId < groupSize, "Instance ID must be < groupSize");
    Preconditions.checkArgument(numGroups > 0, "Number of Groups must be > 0");
    this.groupId = groupId;
    this.instanceId = instanceId;
    this.groupSize = groupSize;
    this.dequeueStrategy = dequeueStrategy;
    this.hashKey = dequeueStrategy == DequeueStrategy.HASH ? hashKey : null;
    this.numGroups = numGroups;
  }

  public long getGroupId() {
    return groupId;
  }

  public int getInstanceId() {
    return instanceId;
  }

  public int getGroupSize() {
    return groupSize;
  }

  public int getNumGroups() {
    return numGroups;
  }
  public DequeueStrategy getDequeueStrategy() {
    return dequeueStrategy;
  }

  public String getHashKey() {
    return hashKey;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("groupId", groupId)
                  .add("instanceId", instanceId)
                  .add("groupSize", groupSize)
                  .add("numGroups", numGroups)
                  .add("dequeueStrategy", dequeueStrategy)
                  .add("hashKey", hashKey)
                  .toString();
  }
}

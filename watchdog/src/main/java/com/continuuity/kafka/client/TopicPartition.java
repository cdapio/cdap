/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import com.google.common.base.Objects;

/**
 * Represents a combination of topic and partition.
 */
public class TopicPartition {

  private final String topic;
  private final int partition;

  public TopicPartition(String topic, int partition) {
    this.topic = topic;
    this.partition = partition;
  }

  public final String getTopic() {
    return topic;
  }

  public final int getPartition() {
    return partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TopicPartition other = (TopicPartition) o;
    return partition == other.partition && topic.equals(other.topic);
  }

  @Override
  public int hashCode() {
    int result = topic.hashCode();
    result = 31 * result + partition;
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("topic", topic)
      .add("partition", partition)
      .toString();
  }
}

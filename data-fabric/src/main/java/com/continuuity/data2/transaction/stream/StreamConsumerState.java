/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.data.stream.StreamFileOffset;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Represents the consumer state of a {@link StreamConsumer}.
 */
public final class StreamConsumerState implements ConsumerState<Iterable<StreamFileOffset>> {

  private final long groupId;
  private final int instanceId;
  private Iterable<StreamFileOffset> state;

  public StreamConsumerState(StreamConsumerState other) {
    this(other.getGroupId(), other.getInstanceId(), other.getState());
  }

  public StreamConsumerState(long groupId, int instanceId) {
    this(groupId, instanceId, ImmutableList.<StreamFileOffset>of());
  }

  public StreamConsumerState(long groupId, int instanceId, Iterable<StreamFileOffset> state) {
    this.groupId = groupId;
    this.instanceId = instanceId;
    this.state = ImmutableList.copyOf(state);
  }

  @Override
  public long getGroupId() {
    return groupId;
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  @Override
  public Iterable<StreamFileOffset> getState() {
    return state;
  }

  @Override
  public void setState(Iterable<StreamFileOffset> state) {
    this.state = ImmutableList.copyOf(state);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StreamConsumerState other = (StreamConsumerState) o;

    return (groupId == other.groupId)
      && (instanceId == other.instanceId)
      && Iterables.elementsEqual(state, other.state);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(groupId, instanceId, state);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("groupId", groupId)
      .add("instanceId", instanceId)
      .add("states", state)
      .toString();
  }
}

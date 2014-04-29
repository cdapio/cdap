/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.data.stream.StreamFileOffset;
import com.continuuity.data.stream.StreamFileType;
import com.continuuity.data.stream.StreamUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 *
 */
public abstract class StreamConsumerStateStoreTest {

  protected abstract StreamConsumerStateStore createStateStore(StreamConfig streamConfig) throws Exception;
  protected abstract StreamAdmin getStreamAdmin();

  @Test
  public void testStore() throws Exception {
    StreamAdmin streamAdmin = getStreamAdmin();
    String streamName = "testStore";
    streamAdmin.create(streamName);

    StreamConfig config = streamAdmin.getConfig(streamName);

    // Creates a state with 4 offsets
    StreamConsumerState state = generateState(0L, 0, config, 0L, 4);
    StreamConsumerStateStore stateStore = createStateStore(config);

    // Save the state.
    stateStore.save(state);

    // Read the state back
    StreamConsumerState readState = stateStore.get(0, 0);
    Assert.assertEquals(state, readState);
  }

  @Test
  public void testMultiStore() throws Exception {
    StreamAdmin streamAdmin = getStreamAdmin();
    String streamName = "testMultiStore";
    streamAdmin.create(streamName);

    StreamConfig config = streamAdmin.getConfig(streamName);

    // Creates 4 states of 2 groups, each with 4 offsets
    Set<StreamConsumerState> states = Sets.newHashSet();
    for (int i = 0; i < 4; i++) {
      states.add(generateState(i % 2, i, config, 0L, 4));
    }

    StreamConsumerStateStore stateStore = createStateStore(config);
    stateStore.save(states);

    // Read all states back
    Set<StreamConsumerState> readStates = Sets.newHashSet();
    stateStore.getAll(readStates);

    Assert.assertEquals(states, readStates);
  }

  @Test
  public void testRemove() throws Exception {
    StreamAdmin streamAdmin = getStreamAdmin();
    String streamName = "testRemove";
    streamAdmin.create(streamName);

    StreamConfig config = streamAdmin.getConfig(streamName);

    // Creates 4 states of 2 groups, each with 4 offsets
    Set<StreamConsumerState> states = Sets.newHashSet();
    for (int i = 0; i < 4; i++) {
      states.add(generateState(i % 2, i, config, 0L, 4));
    }

    StreamConsumerStateStore stateStore = createStateStore(config);
    stateStore.save(states);

    // Read all states back
    Set<StreamConsumerState> readStates = Sets.newHashSet();
    stateStore.getAll(readStates);

    Assert.assertEquals(states, readStates);

    // Remove groupId 0
    Set<StreamConsumerState> removeStates = Sets.newHashSet();
    for (StreamConsumerState state : readStates) {
      if (state.getGroupId() == 0) {
        removeStates.add(state);
      }
    }

    stateStore.remove(removeStates);

    // Read all states back
    readStates.clear();
    stateStore.getAll(readStates);

    Assert.assertEquals(2, readStates.size());

    for (StreamConsumerState state : readStates) {
      Assert.assertEquals(1L, state.getGroupId());
    }
  }

  private StreamConsumerState generateState(long groupId, int instanceId, StreamConfig config,
                                            long partitionBaseTime, int numOffsets) throws IOException {
    List<StreamFileOffset> offsets = Lists.newArrayList();
    long partitionDuration = config.getPartitionDuration();
    for (int i = 0; i < numOffsets; i++) {
      Location partitionLocation = StreamUtils.createPartitionLocation(partitionBaseTime * partitionDuration, config);
      offsets.add(new StreamFileOffset(StreamUtils.createStreamLocation(partitionLocation,
                                                                        "file", 0, StreamFileType.EVENT), i * 1000));
    }

    return new StreamConsumerState(groupId, instanceId, offsets);
  }
}

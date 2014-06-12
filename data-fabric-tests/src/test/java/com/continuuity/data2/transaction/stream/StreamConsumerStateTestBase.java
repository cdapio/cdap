/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data.stream.StreamFileOffset;
import com.continuuity.data.stream.StreamFileType;
import com.continuuity.data.stream.StreamUtils;
import com.google.common.collect.Iterables;
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
public abstract class StreamConsumerStateTestBase {

  protected abstract StreamConsumerStateStore createStateStore(StreamConfig streamConfig) throws Exception;
  protected abstract StreamAdmin getStreamAdmin();

  @Test
  public void testStateExists() throws Exception {
    StreamAdmin streamAdmin = getStreamAdmin();
    String streamName = "testStateExists";
    streamAdmin.create(streamName);

    StreamConfig config = streamAdmin.getConfig(streamName);
    StreamConsumerStateStore stateStore = createStateStore(config);

    streamAdmin.configureInstances(QueueName.fromStream(streamName), 0L, 1);

    // Get a consumer state that is configured
    StreamConsumerState state = stateStore.get(0L, 0);
    Assert.assertNotNull(state);

    // Try to get a consumer state that not configured yet.
    state = stateStore.get(0L, 1);
    Assert.assertNull(state);
  }

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

  @Test
  public void testChangeInstance() throws Exception {
    StreamAdmin streamAdmin = getStreamAdmin();
    String streamName = "testChangeInstance";
    streamAdmin.create(streamName);

    StreamConfig config = streamAdmin.getConfig(streamName);

    // Creates a state with 4 offsets
    StreamConsumerState state = generateState(0L, 0, config, 0L, 4);
    StreamConsumerStateStore stateStore = createStateStore(config);

    // Save the state.
    stateStore.save(state);

    // Increase the number of instances
    streamAdmin.configureInstances(QueueName.fromStream(streamName), 0L, 2);

    StreamConsumerState newState = stateStore.get(0L, 1);
    // Get the state of the new instance, should be the same as the existing one
    Assert.assertTrue(Iterables.elementsEqual(state.getState(), newState.getState()));

    // Change the state of instance 0 to higher offset.
    List<StreamFileOffset> fileOffsets = Lists.newArrayList(state.getState());
    StreamFileOffset fileOffset = fileOffsets.get(0);
    long oldOffset = fileOffset.getOffset();
    long newOffset = oldOffset + 100000;
    fileOffsets.set(0, new StreamFileOffset(fileOffset, newOffset));
    state.setState(fileOffsets);
    stateStore.save(state);

    // Verify the change
    state = stateStore.get(0L, 0);
    Assert.assertEquals(newOffset, Iterables.get(state.getState(), 0).getOffset());

    // Increase the number of instances again
    streamAdmin.configureInstances(QueueName.fromStream(streamName), 0L, 3);

    // Verify that instance 0 has offset getting resetted to lowest
    state = stateStore.get(0L, 0);
    Assert.assertEquals(oldOffset, Iterables.get(state.getState(), 0).getOffset());

    // Verify that no new file offsets state is being introduced (test a bug in the configureInstance implementation)
    Assert.assertEquals(4, Iterables.size(state.getState()));

    // Verify that all offsets are the same
    List<StreamConsumerState> states = Lists.newArrayList();
    stateStore.getByGroup(0L, states);

    Assert.assertEquals(3, states.size());
    Assert.assertTrue(Iterables.elementsEqual(states.get(0).getState(), states.get(1).getState()));
    Assert.assertTrue(Iterables.elementsEqual(states.get(0).getState(), states.get(2).getState()));
  }

  private StreamConsumerState generateState(long groupId, int instanceId, StreamConfig config,
                                            long partitionBaseTime, int numOffsets) throws IOException {
    List<StreamFileOffset> offsets = Lists.newArrayList();
    long partitionDuration = config.getPartitionDuration();
    for (int i = 0; i < numOffsets; i++) {
      Location partitionLocation = StreamUtils.createPartitionLocation(config.getLocation(),
                                                                       (partitionBaseTime + i) * partitionDuration,
                                                                       config.getPartitionDuration());

      offsets.add(new StreamFileOffset(StreamUtils.createStreamLocation(partitionLocation,
                                                                        "file", 0, StreamFileType.EVENT), i * 1000, 0));
    }

    return new StreamConsumerState(groupId, instanceId, offsets);
  }
}

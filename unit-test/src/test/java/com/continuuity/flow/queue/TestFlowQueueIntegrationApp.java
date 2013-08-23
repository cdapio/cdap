/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.flow.queue;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.HashPartition;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.RoundRobin;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.Callback;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Application for testing flow and queue integration.
 */
public class TestFlowQueueIntegrationApp implements Application {

  public static final int MAX_ITERATIONS = 20;

  private static final Logger LOG = LoggerFactory.getLogger(TestFlowQueueIntegrationApp.class);
  private static final String HASH_KEY1 = "hkey1";
  private static final String HASH_KEY2 = "hkey2";

  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}.
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("TestFlowQueueIntegrationApp")
      .setDescription("Application for testing queue partitioning")
      .withStreams().add(new Stream("s1"))
      .noDataSet()
      .withFlows().add(new QueuePartitionFlow())
      .noProcedure().build();
  }

  /**
   * Queue partitioning flow.
   */
  public static class QueuePartitionFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("QueuePartitionFlow")
        .setDescription("Flow for testing queue partitioning")
        .withFlowlets().add(new StreamReader())
                       .add(new QueuePartitionTestFlowlet(), QueuePartitionTestFlowlet.NUM_INSTANCES)
                       .add(new QueueBatchTestFlowlet(), QueueBatchTestFlowlet.NUM_INSTANCES)
        .connect().from("StreamReader").to("QueuePartitionTestFlowlet")
        .from("StreamReader").to("QueueBatchTestFlowlet")
        .fromStream("s1").to("StreamReader")
        .build();
    }
  }

  /**
   * StreamReader flowlet.
   */
  public static class StreamReader extends AbstractFlowlet {
    @Output("int")
    private OutputEmitter<Integer> output;
    @Output("entry1")
    private OutputEmitter<Entry1> output1;
    @Output("entry2")
    private OutputEmitter<Entry2> output2;
    @Output("batch_entry")
    private OutputEmitter<BatchEntry> batchOutput;

    @ProcessInput
    public void foo(StreamEvent event) {
      final int input = Integer.parseInt(Charsets.UTF_8.decode(event.getBody()).toString());
      LOG.warn("Writing " + input);
      output.emit(input, HASH_KEY1, input + 1);
      output1.emit(new Entry1(Entry1.ID + input), HASH_KEY1, input + 1);
      output2.emit(new Entry2(Entry2.ID + input), HASH_KEY2, input + 2);

      // Emit batch output in one shot in the beginning
      if (input == 0) {
        for (int j = 0; j < MAX_ITERATIONS; ++j) {
          batchOutput.emit(new BatchEntry(BatchEntry.ID + j), HASH_KEY1, j);
        }
      }
    }
  }

  /**
   * Flowlet to test queue partitioning.
   */
  public static class QueuePartitionTestFlowlet extends AbstractFlowlet implements Callback {
    public static final int NUM_INSTANCES = 3;
    private final int id = new Random(System.currentTimeMillis()).nextInt(100);

    // Map from first element to a sequence of elements
    // They are ok to be static since in unit-test it's all in the same VM
    private static final Map<Integer, List<Integer>> EXPECTED_ROUND_ROBIN_LISTS;
    private static final Map<Integer, List<Integer>> EXPECTED_HASH1_LISTS;
    private static final Map<Integer, List<Integer>> EXPECTED_HASH2_LISTS;

    static {
      EXPECTED_ROUND_ROBIN_LISTS = Maps.newHashMap();
      EXPECTED_ROUND_ROBIN_LISTS.put(0, ImmutableList.of(0, 3, 6, 9, 12, 15, 18));
      EXPECTED_ROUND_ROBIN_LISTS.put(1, ImmutableList.of(1, 4, 7, 10, 13, 16, 19));
      EXPECTED_ROUND_ROBIN_LISTS.put(2, ImmutableList.of(2, 5, 8, 11, 14, 17));

      EXPECTED_HASH1_LISTS = Maps.newHashMap();
      EXPECTED_HASH1_LISTS.put(0, ImmutableList.of(0, 3, 6, 9, 12, 15, 18));
      EXPECTED_HASH1_LISTS.put(1, ImmutableList.of(1, 4, 7, 10, 13, 16, 19));
      EXPECTED_HASH1_LISTS.put(2, ImmutableList.of(2, 5, 8, 11, 14, 17));

      EXPECTED_HASH2_LISTS = Maps.newHashMap();
      EXPECTED_HASH2_LISTS.put(0, ImmutableList.of(0, 3, 6, 9, 12, 15, 18));
      EXPECTED_HASH2_LISTS.put(1, ImmutableList.of(1, 4, 7, 10, 13, 16, 19));
      EXPECTED_HASH2_LISTS.put(2, ImmutableList.of(2, 5, 8, 11, 14, 17));
    }

    private Iterator<Integer> expectedRoundRobinIterator = null;
    private Iterator<Integer> expectedHash1Iterator = null;
    private Iterator<Integer> expectedHash2Iterator = null;

    @ProcessInput("int")
    @RoundRobin
    public void roundRobin(int actual) {
      LOG.warn("RID:" + id + " value=" + actual);
      if (expectedRoundRobinIterator == null) {
        if (!EXPECTED_ROUND_ROBIN_LISTS.containsKey(actual)) {
          Assert.fail("Unexpected sequence " + actual);
        }
        expectedRoundRobinIterator = EXPECTED_ROUND_ROBIN_LISTS.remove(actual).iterator();
      }
      verify(0, actual, expectedRoundRobinIterator);
    }

    @ProcessInput("entry1")
    @HashPartition(HASH_KEY1)
    public void hashPartition1(Entry1 actual) {
      if (expectedHash1Iterator == null) {
        if (!EXPECTED_HASH1_LISTS.containsKey(actual.i - Entry1.ID)) {
          Assert.fail("Unexpected sequence " + actual.i);
        }
        expectedHash1Iterator = EXPECTED_HASH1_LISTS.remove(actual.i - Entry1.ID).iterator();
      }
      verify(Entry1.ID, actual.i, expectedHash1Iterator);
    }

    @ProcessInput("entry2")
    @HashPartition(HASH_KEY2)
    public void hashPartition2(Entry2 actual) {
      if (expectedHash2Iterator == null) {
        if (!EXPECTED_HASH2_LISTS.containsKey(actual.i - Entry2.ID)) {
          Assert.fail("Unexpected sequence " + actual.i);
        }
        expectedHash2Iterator = EXPECTED_HASH2_LISTS.remove(actual.i - Entry2.ID).iterator();
      }
      verify(Entry2.ID, actual.i, expectedHash2Iterator);

    }

    private void verify(int base, int i, Iterator<Integer> iterator) {
      if (iterator.hasNext()) {
        int expected = iterator.next();
        Assert.assertEquals(expected, i - base);
      } else {
        Assert.fail("Unexpected sequence " + i);
      }
    }

    @Override
    public void onSuccess(@Nullable Object input, @Nullable InputContext inputContext) {
    }

    @Override
    public FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason reason) {
      return FailurePolicy.IGNORE;
    }
  }

  /**
   * Flowlet to test queue batches.
   */
  public static class QueueBatchTestFlowlet extends AbstractFlowlet implements Callback {
    public static final int NUM_INSTANCES = 5;
    private int id = new Random(System.currentTimeMillis()).nextInt(100);
    private int first = -1;

    private final List<Integer>[] expectedLists;
    private Iterator<Integer> expectedListIterator;

    public QueueBatchTestFlowlet() {
      //noinspection unchecked
      expectedLists = new List[5];

      expectedLists[0] = ImmutableList.of(0, 5, 10, 15);
      expectedLists[1] = ImmutableList.of(1, 6, 11, 16);
      expectedLists[2] = ImmutableList.of(2, 7, 12, 17);
      expectedLists[3] = ImmutableList.of(3, 8, 13, 18);
      expectedLists[4] = ImmutableList.of(4, 9, 14, 19);
    }

    @ProcessInput("batch_entry")
    @HashPartition(HASH_KEY1)
    @Batch(100)
    public void process(Iterator<BatchEntry> it) {
      List<BatchEntry> actualList = ImmutableList.copyOf(it);
      LOG.warn("HID:" + id + " batch values=" + actualList);

      if (first == -1) {
        first = actualList.get(0).i - BatchEntry.ID;
        expectedListIterator = expectedLists[first].iterator();
        for (BatchEntry actual : actualList) {
          Assert.assertEquals((int) expectedListIterator.next(), actual.i - BatchEntry.ID);
        }
        Assert.assertFalse(expectedListIterator.hasNext());
      } else {
        Assert.fail("HID:" + id + " Batch dequeue is not working correctly. Got more than one batch - " + actualList);
      }
    }

    @Override
    public void onSuccess(@Nullable Object input, @Nullable InputContext inputContext) {
    }

    @Override
    public FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason reason) {
      return FailurePolicy.IGNORE;
    }
  }

  private static class Entry1 {
    public int i;
    public static final int ID = 200;

    private Entry1(int i) {
      this.i = i;
    }

    @Override
    public String toString() {
      return "Entry1{" +
        "i=" + i +
        '}';
    }
  }

  private static class Entry2 {
    public int i;
    public static final int ID = 500;

    private Entry2(int i) {
      this.i = i;
    }

    @Override
    public String toString() {
      return "Entry2{" +
        "i=" + i +
        '}';
    }
  }

  @Nonnull
  private static class BatchEntry {
    public int i;
    public static final int ID = 1000;
    public String batchId = "batchID";

    private BatchEntry(int i) {
      this.i = i;
    }

    @Override
    public String toString() {
      return "BatchEntry{" +
        "i=" + i +
        '}';
    }
  }
}

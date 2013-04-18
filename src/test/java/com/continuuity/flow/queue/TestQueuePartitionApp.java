/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.flow.queue;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
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
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * This is a sample word count app that is used in testing in
 * many places.
 */
public class TestQueuePartitionApp implements Application {

  private static final Logger LOG = LoggerFactory.getLogger(TestQueuePartitionApp.class);
  private static final String HASH_KEY1 = "hkey1";
  private static final String HASH_KEY2 = "hkey2";

  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("TestQueuePartitionApp")
      .setDescription("Application for testing queue partitioning")
      .withStreams().add(new Stream("s1"))
      .noDataSet()
      .withFlows().add(new QueuePartitionFlow())
      .noProcedure().build();
  }

  public static class QueuePartitionFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("QueuePartitionFlow")
        .setDescription("Flow for testing queue partitioning")
        .withFlowlets().add(new StreamReader())
                       .add(new Flowlet1())
                       .add(new Flowlet2())
        .connect().from("StreamReader").to("Flowlet1")
        .from("StreamReader").to("Flowlet2")
        .fromStream("s1").to("StreamReader")
        .build();
    }
  }


  public static class StreamReader extends AbstractFlowlet {
    @Output("int")
    private OutputEmitter<Integer> output;
    @Output("entry1")
    private OutputEmitter<Entry1> output1;
    @Output("entry2")
    private OutputEmitter<Entry2> output2;

    @ProcessInput
    public void foo(StreamEvent event) {
      int i = Integer.parseInt(Charsets.UTF_8.decode(event.getBody()).toString());
      LOG.warn("Writing " + i);
      output.emit(i, HASH_KEY1, i + 1);
      output1.emit(new Entry1(i), HASH_KEY1, i + 1);
      output2.emit(new Entry2(i), HASH_KEY2, i + 2);
    }
  }

  public static class Flowlet1 extends AbstractFlowlet implements Callback {
    public static final int NUM_INSTANCES = 3;
    private final int id = new Random(System.currentTimeMillis()).nextInt(100);

    private final List<Integer>[] expectedRoundRobinLists;
    private Iterator<Integer> expectedRoundRobinIterator = null;

    private final List<Integer>[] expectedHash1Lists;
    private Iterator<Integer> expectedHash1Iterator = null;

    private final List<Integer>[] expectedHash2Lists;
    private Iterator<Integer> expectedHash2Iterator = null;

    private volatile int first = -1;
    private CountDownLatch latch = new CountDownLatch(1);

    @SuppressWarnings("unchecked")
    public Flowlet1() {
      expectedRoundRobinLists = new List[NUM_INSTANCES];
      expectedRoundRobinLists[0] = ImmutableList.of(0, 3, 6, 9, 12, 15, 18);
      expectedRoundRobinLists[1] = ImmutableList.of(1, 4, 7, 10, 13, 16, 19);
      expectedRoundRobinLists[2] = ImmutableList.of(2, 5, 8, 11, 14, 17);

      expectedHash1Lists = new List[NUM_INSTANCES];
      expectedHash1Lists[0] = ImmutableList.of(0, 3, 6, 9, 12, 15, 18);
      expectedHash1Lists[1] = ImmutableList.of(1, 4, 7, 10, 13, 16, 19);
      expectedHash1Lists[2] = ImmutableList.of(2, 5, 8, 11, 14, 17);

      expectedHash2Lists = new List[NUM_INSTANCES];
      expectedHash2Lists[0] = ImmutableList.of(2, 5, 8, 11, 14, 17);
      expectedHash2Lists[1] = ImmutableList.of(0, 3, 6, 9, 12, 15, 18);
      expectedHash2Lists[2] = ImmutableList.of(1, 4, 7, 10, 13, 16, 19);
    }

    @ProcessInput("int")
    @RoundRobin
    public void roundRobin(int actual) {
      LOG.warn("RID:" + id + " value=" + actual);
      if(expectedRoundRobinIterator == null) {
        expectedRoundRobinIterator = expectedRoundRobinLists[actual].iterator();
        first = actual;
        latch.countDown();
      }
      if(expectedRoundRobinIterator.hasNext()) {
        int expected = expectedRoundRobinIterator.next();
        Assert.assertEquals(expected, actual);
      } else {
        Assert.fail("Not enough inputs!");
      }
    }

    @ProcessInput("entry1")
    @HashPartition(HASH_KEY1)
    public void hashPartition1(Entry1 actual) {
      String logID = "HID1:" + id;

      // Wait for first entry to be populated
      try {
        latch.await();
      } catch (InterruptedException e) {
        Assert.fail(logID + ": Interrupted latch wait");
      }

      if(expectedHash1Iterator == null) {
        expectedHash1Iterator = expectedHash1Lists[first].iterator();
      }
      verify(actual.i,expectedHash1Iterator, logID);
    }

    @ProcessInput("entry2")
    @HashPartition(HASH_KEY2)
    public void hashPartition2(Entry2 actual) {
      String logID = "HID2:" + id;

      // Wait for first entry to be populated
      try {
        latch.await();
      } catch (InterruptedException e) {
        Assert.fail(logID + ": Interrupted latch wait");
      }

      if(expectedHash2Iterator == null) {
        expectedHash2Iterator = expectedHash2Lists[first].iterator();
      }
      verify(actual.i,expectedHash2Iterator, logID);
    }

    public void verify(int i, Iterator<Integer> iterator, String logID) {
      logID = logID  + " : first=" + first;
      LOG.warn(logID + ": value=" + i);
      if(iterator.hasNext()) {
        int expected = iterator.next();
        Assert.assertEquals(logID, expected, i);
      } else {
        Assert.fail(logID + ": Not enough inputs!");
      }
    }

    @Override
    public void onSuccess(@Nullable Object input, @Nullable InputContext inputContext) {
    }

    @Override
    public FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason reason) {
      return FailurePolicy.RETRY;
    }
  }

  public static class Flowlet2 extends AbstractFlowlet implements Callback {
    public static final int NUM_INSTANCES = 5;
    private int prev = -1;
    private int id = new Random(System.currentTimeMillis()).nextInt(100);

    @ProcessInput("int")
    @HashPartition(HASH_KEY1)
    public void foo(Integer i) {
      LOG.warn("HID:" + id + " value=" + i);
      if(prev != -1) {
        Assert.assertEquals(prev + NUM_INSTANCES, (int) i);
      }
      prev = i;
    }

    @Override
    public void onSuccess(@Nullable Object input, @Nullable InputContext inputContext) {
    }

    @Override
    public FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason reason) {
      return FailurePolicy.RETRY;
    }
  }

  private static class Entry1 {
    public int i;

    private Entry1(int i) {
      this.i = i;
    }
  }

  private static class Entry2 {
    public int i;

    private Entry2(int i) {
      this.i = i;
    }
  }
}

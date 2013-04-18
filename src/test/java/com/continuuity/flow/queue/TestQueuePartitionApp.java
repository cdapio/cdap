/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.flow.queue;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.HashPartition;
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
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Random;

/**
 * This is a sample word count app that is used in testing in
 * many places.
 */
public class TestQueuePartitionApp implements Application {

  private static final Logger LOG = LoggerFactory.getLogger(TestQueuePartitionApp.class);
  private static final String HASH_KEY = "hkey";
  public static final int RR_NUM_INSTANCES = 3;
  public static final int HASH_NUM_INSTANCES = 5;

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
                       .add(new RoundRobinFlowlet())
                       .add(new HashPartitionFlowlet())
        .connect().from("StreamReader").to("RoundRobinFlowlet")
        .from("StreamReader").to("HashPartitionFlowlet")
        .fromStream("s1").to("StreamReader")
        .build();
    }
  }


  public static class StreamReader extends AbstractFlowlet {
    private OutputEmitter<Integer> output;

    @ProcessInput
    public void foo(StreamEvent event) {
      int i = Integer.parseInt(Charsets.UTF_8.decode(event.getBody()).toString());
      LOG.warn("Writing " + i);
      output.emit(i, HASH_KEY, i);
    }
  }

  public static class RoundRobinFlowlet extends AbstractFlowlet implements Callback {
    private int prev = -1;
    private int id = new Random(System.currentTimeMillis()).nextInt(100);

    @ProcessInput
    @RoundRobin
    public void foo(Integer i) {
      LOG.warn("RID:" + id + " value=" + i);
      if(prev != -1) {
        Assert.assertEquals(prev + RR_NUM_INSTANCES, (int) i);
      }
      prev = i;
    }

    @Override
    public void onSuccess(@Nullable Object input, @Nullable InputContext inputContext) {
    }

    @Override
    public FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason reason) {
      return FailurePolicy.IGNORE;
    }
  }

  public static class HashPartitionFlowlet extends AbstractFlowlet implements Callback {
    private int prev = -1;
    private int id = new Random(System.currentTimeMillis()).nextInt(100);

    @ProcessInput
    @HashPartition(key=HASH_KEY)
    public void foo(Integer i) {
      LOG.warn("HID:" + id + " value=" + i);
      if(prev != -1) {
        Assert.assertEquals(prev + HASH_NUM_INSTANCES, (int) i);
      }
      prev = i;
    }

    @Override
    public void onSuccess(@Nullable Object input, @Nullable InputContext inputContext) {
    }

    @Override
    public FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason reason) {
      return FailurePolicy.IGNORE;
    }
  }
}

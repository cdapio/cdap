package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.RoundRobin;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletException;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Bench App.
 */
public class BenchApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("BenchApp")
      .setDescription("Bench application")
      .noStream()
      .noDataSet()
      .withFlows()
      .add(new BenchFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   *
   */
  private static class BenchFlow implements Flow {
    public static final int BATCH_SIZE = 100;

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("BenchFlow")
        .setDescription("Benchmark flow")
        .withFlowlets()
        .add("Source", (new Source()), 1)
//        .add("Source", (new SleepingSource()), 1)
        .add("Transfer1", (new Transfer()), 1)
        .add("Transfer2", (new Transfer()), 1)
        .add("Transfer3", (new Transfer()), 1)
        .add("Transfer4", (new Transfer()), 1)
        .add("Transfer5", (new Transfer()), 1)
          // keeping to prevent eviction
//        .add("Destination2", (new SleepingDestination()), 1)
        .add("Destination", (new Destination()), 1)
        .connect()
        .from("Source").to("Transfer1")
        .from("Transfer1").to("Transfer2")
        .from("Transfer2").to("Transfer3")
        .from("Transfer3").to("Transfer4")
        .from("Transfer4").to("Transfer5")
        .from("Transfer5").to("Destination")
//        .from("Source").to("Destination2")
        .build();
    }
  }

  /**
   *
   */
  private static class Source extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(Source.class);

    private static final String OUT = "Out";

    public static final int MESSAGE_SIZE = 256;

    private final Random random = new Random();

    private OutputEmitter<String> out;

    private int produced = 0;
    private long lastReported;
    private int instanceId;

    @Override
    public void initialize(FlowletContext context) throws FlowletException {
      this.instanceId = context.getInstanceId();
    }

    @Tick(delay = 1L, unit = TimeUnit.NANOSECONDS)
    public void generate() throws Exception {
      for (int i = 0; i < BenchFlow.BATCH_SIZE; i++) {
        out.emit(createMessage());
      }
      produced += BenchFlow.BATCH_SIZE;

      long now = System.currentTimeMillis();
      if (now - lastReported > TimeUnit.SECONDS.toMillis(1)) {
        lastReported = now;
        LOG.info("Flowlet " + instanceId + " produced so far: " + produced);
      }

      if (produced % (BenchFlow.BATCH_SIZE * 10) == 0) {
        LOG.info("Flowlet " + instanceId + " produced so far: " + produced);
      }

      throttle();
    }

    private String createMessage() {
      byte[] bytes = new byte[MESSAGE_SIZE];
      random.nextBytes(bytes);
      return Bytes.toString(bytes);
    }

    private void throttle() throws InterruptedException {
      // todo: need better logic to throttle
      // to throttle data at max per second
      long maxPerSecond = 6000;
      TimeUnit.MILLISECONDS.sleep(BenchFlow.BATCH_SIZE * 1000 / maxPerSecond);
    }

  }

  /**
   *
   */
  private static class Destination extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(Destination.class);
    private int processed = 0;
    private int batches = 0;
    private long lastReported;
    private int instanceId;

    @Override
    public void initialize(FlowletContext context) throws FlowletException {
      this.instanceId = context.getInstanceId();
    }

    @Batch(BenchFlow.BATCH_SIZE)
    @RoundRobin
    public void process(Iterator<String> out) throws Exception {
      batches++;
      int thisBatch = 0;
      while (out.hasNext()) {
        out.next();
        thisBatch++;
      }

      processed += thisBatch;

      long now = System.currentTimeMillis();
      if (now - lastReported > TimeUnit.SECONDS.toMillis(1)) {
        lastReported = now;
        LOG.info("Flowlet " + instanceId + " processed so far: " + processed +
                   ", " + (processed / batches) + " per batch with " + thisBatch + " in last batch");
      }
    }

  }

  /**
   *
   */
  private static class Transfer extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(Transfer.class);
    private int processed = 0;
    private int batches = 0;
    private long lastReported;
    private int instanceId;

    private OutputEmitter<String> out;

    @Override
    public void initialize(FlowletContext context) throws FlowletException {
      this.instanceId = context.getInstanceId();
    }

    @Batch(BenchFlow.BATCH_SIZE)
    @RoundRobin
    public void process(Iterator<String> input) throws Exception {
      batches++;
      int thisBatch = 0;
      while (input.hasNext()) {
        out.emit(input.next());
        thisBatch++;
      }

      processed += thisBatch;

      long now = System.currentTimeMillis();
      if (now - lastReported > TimeUnit.SECONDS.toMillis(1)) {
        lastReported = now;
        LOG.info("Flowlet " + instanceId + " processed so far: " + processed +
                   ", " + (processed / batches) + " per batch with " + thisBatch + " in last batch");
      }
    }

  }
}



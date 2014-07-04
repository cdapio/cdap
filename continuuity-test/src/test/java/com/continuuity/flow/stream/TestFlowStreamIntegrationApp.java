package com.continuuity.flow.stream;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Flow stream integration tests.
 */
public class TestFlowStreamIntegrationApp implements Application {
  private static final Logger LOG = LoggerFactory.getLogger(TestFlowStreamIntegrationApp.class);

  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}.
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("TestFlowStreamIntegrationApp")
      .setDescription("Application for testing batch stream dequeue")
      .withStreams().add(new Stream("s1"))
      .noDataSet()
      .withFlows().add(new StreamTestFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   * Stream test flow.
   */
  public static class StreamTestFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("StreamTestFlow")
        .setDescription("Flow for testing batch stream dequeue")
        .withFlowlets().add(new StreamReader())
        .connect().fromStream("s1").to("StreamReader")
        .build();
    }
  }

  /**
   * StreamReader flowlet.
   */
  public static class StreamReader extends AbstractFlowlet {

    @ProcessInput
    @Batch(100)
    public void foo(Iterator<StreamEvent> it) {
      List<StreamEvent> events = ImmutableList.copyOf(it);
      LOG.warn("Number of batched stream events = " + events.size());
      Preconditions.checkState(events.size() > 1);

      List<Integer> out = Lists.newArrayList();
      for (StreamEvent event : events) {
        out.add(Integer.parseInt(Charsets.UTF_8.decode(event.getBody()).toString()));
      }
      LOG.info("Read events=" + out);
    }
  }
}

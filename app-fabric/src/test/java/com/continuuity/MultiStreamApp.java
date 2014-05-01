/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.table.Get;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This is a sample word count app that is used in testing in
 * many places.
 */
public class MultiStreamApp implements Application {

  private static final Logger LOG = LoggerFactory.getLogger(MultiStreamApp.class);

  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}.
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("MultiStreamApp")
      .setDescription("Application for testing changing stream-flowlet connections")
      .withStreams()
        .add(new Stream("stream1")).add(new Stream("stream2"))
        .add(new Stream("stream3")).add(new Stream("stream4"))
      .withDataSets().add(new Table("table"))
      .withFlows().add(new CounterFlow())
      .withProcedures().add(new CountersProcedure())
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   *
   */
  public static class CounterFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("CounterFlow")
        .setDescription("Flow for counting events")
        .withFlowlets().add("counter1", new EventsCounter1())
                       .add("counter2", new EventsCounter2())
        .connect()
          .fromStream("stream1").to("counter1")
          .fromStream("stream3").to("counter2")
        .build();
    }
  }

  /**
   *
   */
  public static class EventsCounter1 extends AbstractFlowlet {
    @UseDataSet("table")
    private Table table;

    @ProcessInput()
    public void process(StreamEvent event) {
      table.increment(new Increment("row", "counter1", 1));
    }
  }

  /**
   *
   */
  public static class EventsCounter2 extends AbstractFlowlet {
    @UseDataSet("table")
    private Table table;

    @ProcessInput("stream3")
    public void process(StreamEvent event) {
      table.increment(new Increment("row", "counter2", 1));
    }
  }

  /**
   *
   */
  public static class CountersProcedure extends AbstractProcedure {
    @UseDataSet("table")
    private Table table;

    @Handle("get")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      responder.sendJson(table.get(new Get("row")).getLong("column"));
    }
  }
}

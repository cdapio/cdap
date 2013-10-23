/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.apps.testing;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * This is a Continuuity App that is used for performance testing the new distributed logging.
 */
public class LoggingApp implements Application {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingApp.class);

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("LoggingApp")
      .setDescription("Logging generated numbers")
      .noStream()
      .noDataSet()
      .withFlows().add(new LoggingFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  private static class LoggingFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("LoggingFlow")
        .setDescription("Flow that generates random numbers and logs them")
        .withFlowlets()
        .add("source", new NumberSource())
        .add("logger", new LoggerFlowlet())
        .connect()
        .from("source").to("logger")
        .build();
    }
  }

  private static class LoggerFlowlet extends AbstractFlowlet {

    public LoggerFlowlet() {
      super("LoggerFlowlet");
    }

    public void process(Integer number) {
      LOG.info(number.toString());
    }
  }

  private static class NumberSource extends AbstractFlowlet {
    private OutputEmitter<Integer> randomOutput;

    private Random random;


    public NumberSource() {
      random = new Random();
    }

    @Tick(delay = 10, unit = TimeUnit.MILLISECONDS)
    public void generate() throws Exception {
      Integer randomNumber = new Integer(this.random.nextInt(10000));
      randomOutput.emit(randomNumber);
    }
  }
}

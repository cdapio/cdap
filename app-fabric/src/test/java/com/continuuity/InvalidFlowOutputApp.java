/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class InvalidFlowOutputApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("InvalidFlowOutputApp")
      .setDescription("Invalid Flow output app")
      .noStream()
      .noDataSet()
      .withFlows()
        .add(new InvalidFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   *
   */
  public static final class InvalidFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("InvalidFlow")
        .setDescription("Invalid flow")
        .withFlowlets()
          .add("gen", new InvalidGenerator())
          .add("cons", new Consumer())
        .connect()
          .from("gen").to("cons")
        .build();
    }
  }

  /**
   *
   */
  public static final class InvalidGenerator extends AbstractFlowlet {

    private OutputEmitter<String> strOut;
    private OutputEmitter<Long> longOut;


    @Tick(delay = 1L, unit = TimeUnit.SECONDS)
    public void generate() {
      long ts = System.currentTimeMillis();
      strOut.emit(Long.toString(ts));
      longOut.emit(ts);
    }
  }

  /**
   *
   */
  public static final class Consumer extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    public void process(String str) {
      LOG.info(str);
    }

    public void process(long ts) {
      LOG.info(Long.toString(ts));
    }
  }
}

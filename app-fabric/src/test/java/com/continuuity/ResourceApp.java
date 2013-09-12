/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.internal.DefaultResourceSpecification;

/**
 * This is an Application used for only testing that sets various resources for different
 * flowlets and procedures.
 */
public class ResourceApp implements Application {
  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}.
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("ResourceApp")
      .setDescription("Resource Application")
      .withStreams().add(new Stream("X"))
      .noDataSet()
      .withFlows().add(new ResourceFlow())
      .withProcedures()
        .add(new DummyProcedure())
      .noBatch()
      .build();
  }

  private class DummyProcedure extends AbstractProcedure {
    @Override
    public ProcedureSpecification configure() {
      return ProcedureSpecification.Builder.with()
        .setName("dummy")
        .setDescription("dummy procedure")
        .withResources(new DefaultResourceSpecification(128, 3))
        .build();
    }
  }

  /**
   *
   */
  public static final class ResourceFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("ResourceFlow")
        .setDescription("Simple Resource Flow")
        .withFlowlets()
          .add("A", new A())
          .add("B", new B())
        .connect()
          .fromStream("X").to("A")
          .fromStream("X").to("B")
        .build();
    }
  }

  /**
   *
   */
  public static final class A extends AbstractFlowlet {

    public A() {
      super("A");
    }

    @Override
    public FlowletSpecification configure() {
      return FlowletSpecification.Builder.with()
        .setName("A")
        .setDescription("A flowlet")
        .withResources(new DefaultResourceSpecification(1024, 2))
        .build();
    }

    public void process(StreamEvent event) {
    }
  }

  /**
   *
   */
  public static final class B extends AbstractFlowlet {

    public B() {
      super("B");
    }

    @Override
    public FlowletSpecification configure() {
      return FlowletSpecification.Builder.with()
        .setName("B")
        .setDescription("B flowlet")
        .withResources(new DefaultResourceSpecification(2048, 5))
        .build();
    }

    public void process(StreamEvent event) {
    }
  }
}

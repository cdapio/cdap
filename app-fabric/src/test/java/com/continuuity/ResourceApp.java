/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.procedure.AbstractProcedure;

/**
 * This is an Application used for only testing that sets various resources for different
 * flowlets and procedures.
 */
public class ResourceApp implements Application {
  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}
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
        .add(new DummyProcedure()).setVirtualCores(3).setMemoryMB(128)
      .noBatch()
      .build();
  }

  private class DummyProcedure extends AbstractProcedure {}

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
          .add("A", new A()).setVirtualCores(2).setMemoryMB(1024)
          .add("B", new B()).setVirtualCores(5).setMemory(2, ResourceSpecification.SizeUnit.GIGA)
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

    public void process(StreamEvent event) {
    }
  }
}

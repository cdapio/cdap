/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureSpecification;

/**
 * This is an Application used for only testing that sets various resources for different
 * flowlets and procedures.
 */
public class ResourceApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("ResourceApp");
    setDescription("Resource Application");
    addStream(new Stream("X"));
    addFlow(new ResourceFlow());
    addProcedure(new DummyProcedure());
    addMapReduce(new DummyBatch());
  }

  private class DummyProcedure extends AbstractProcedure {
    @Override
    public ProcedureSpecification configure() {
      return ProcedureSpecification.Builder.with()
        .setName("dummy")
        .setDescription("dummy procedure")
        .withResources(new Resources(128, 3))
        .build();
    }
  }

  /**
   * Some flow
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
   * A map/reduce job.
   */
  public static class DummyBatch extends AbstractMapReduce {
    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("dummy-batch")
        .setDescription("dummy mapred job")
        .setMapperMemoryMB(512)
        .setReducerMemoryMB(1024)
        .build();
    }
  }

  /**
   * A dummy flowlet
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
        .withResources(new Resources(1024, 2))
        .build();
    }

    public void process(StreamEvent event) {
    }
  }

  /**
   * Another dummy flowlet
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
        .withResources(new Resources(2048, 5))
        .build();
    }

    public void process(StreamEvent event) {
    }
  }
}

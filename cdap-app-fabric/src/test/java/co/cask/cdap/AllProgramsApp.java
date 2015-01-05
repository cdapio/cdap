/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;

import java.io.IOException;

/**
 * App that contains all program types. Used to test Metadata store.
 */
public class AllProgramsApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("App");
    setDescription("Application which has everything");
    addStream(new Stream("stream"));
    createDataset("kvt", KeyValueTable.class);
    addFlow(new NoOpFlow());
    addProcedure(new NoOpProcedure());
    addMapReduce(new NoOpMR());
    addWorkflow(new NoOpWorkflow());
  }

  /**
   *
   */
  public static class NoOpFlow implements Flow {
    @Override
    public FlowSpecification configure() {
     return FlowSpecification.Builder.with()
        .setName("NoOpFlow")
        .setDescription("NoOpflow")
        .withFlowlets()
          .add(new A())
        .connect()
          .fromStream("stream").to("A")
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
  }

  /**
   *
   */
  private static class NoOpProcedure extends AbstractProcedure {
    @UseDataSet("kvt")
    private KeyValueTable counters;

    @Handle("dummy")
    public void handle(ProcedureRequest request,
                       ProcedureResponder responder)
      throws IOException {
      responder.sendJson("OK");
    }
  }

  /**
   *
   */
  public static class NoOpMR extends AbstractMapReduce {
  }

  /**
   *
   */
  private static class NoOpWorkflow extends AbstractWorkflow {
    @Override
    public void configure() {
        setName("NoOpWorkflow");
        setDescription("NoOp workflow description");
        addAction(new NoOpAction());
    }
  }

  /**
   *
   */
  private static class NoOpAction extends AbstractWorkflowAction {
    @Override
    public void run() {

    }
  }

}

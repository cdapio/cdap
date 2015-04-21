/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * App that contains all program types. Used to test Metadata store.
 */
public class AllProgramsApp extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(AllProgramsApp.class);

  public static final String NAME = "App";
  public static final String STREAM_NAME = "stream";
  public static final String DATASET_NAME = "kvt";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Application which has everything");
    addStream(new Stream(STREAM_NAME));
    createDataset(DATASET_NAME, KeyValueTable.class);
    addFlow(new NoOpFlow());
    addMapReduce(new NoOpMR());
    addWorkflow(new NoOpWorkflow());
    addWorker(new NoOpWorker());
  }

  /**
   *
   */
  public static class NoOpFlow implements Flow {

    public static final String NAME = "NoOpFlow";

    @Override
    public FlowSpecification configure() {
     return FlowSpecification.Builder.with()
        .setName(NAME)
        .setDescription("NoOpflow")
        .withFlowlets()
          .add(A.NAME, new A())
        .connect()
          .fromStream(STREAM_NAME).to(A.NAME)
        .build();
    }
  }

  /**
   *
   */
  public static final class A extends AbstractFlowlet {
    public static final String NAME = "A";

    public A() {
      super(NAME);
    }

    @ProcessInput
    public void process(StreamEvent event) {
      // NO-OP
    }
  }

  /**
   *
   */
  public static class NoOpMR extends AbstractMapReduce {
    public static final String NAME = "NoOpMR";

    @Override
    protected void configure() {
      setName(NAME);
    }
  }

  /**
   *
   */
  public static class NoOpWorkflow extends AbstractWorkflow {

    public static final String NAME = "NoOpWorkflow";

    @Override
    public void configure() {
        setName(NAME);
        setDescription("NoOp Workflow description");
        addAction(new NoOpAction());
    }
  }

  /**
   *
   */
  public static class NoOpAction extends AbstractWorkflowAction {

    @Override
    public void run() {

    }
  }

  /**
   *
   */
  public static class NoOpWorker extends AbstractWorker {

    public static final String NAME = "NoOpWorker";

    @Override
    public void configure() {
      setName(NAME);
    }

    @Override
    public void run() {
      try {
        getContext().write(STREAM_NAME, ByteBuffer.wrap(Bytes.toBytes("NO-OP")));
      } catch (Exception e) {
        LOG.error("Worker ran into error", e);
      }
    }
  }

}

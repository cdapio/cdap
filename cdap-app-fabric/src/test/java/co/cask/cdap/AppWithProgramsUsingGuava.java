/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;
import com.google.common.base.Preconditions;

/**
 * App that with programs that use a guava library (Preconditions) in their initialize method.
 * Used to test the fix for CDAP-2543.
 */
public class AppWithProgramsUsingGuava extends AbstractApplication {

  public static final String NAME = "App";
  public static final String STREAM_NAME = "stream";
  public static final String DATASET_NAME = "kvt";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Application which has everything");
    addMapReduce(new NoOpMR());
    addWorkflow(new NoOpWorkflow());
    addWorker(new NoOpWorker());
    addSpark(new NoOpSpark());
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

    @Override
    public void initialize() throws Exception {
      Preconditions.checkArgument(true);
    }
  }

  /**
   *
   */
  public static class NoOpSpark extends AbstractSpark {
    public static final String NAME = "NoOpSpark";

    @Override
    protected void configure() {
      setName(NAME);
      setMainClassName("MainClass does not matter because this program fails in initialize()");
    }

    @Override
    protected void initialize() throws Exception {
      Preconditions.checkArgument(true);
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

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      if (context.getRuntimeArguments().containsKey("fail.in.workflow.initialize")) {
        Preconditions.checkArgument(true);
      }
    }
  }

  /**
   *
   */
  public static class NoOpAction extends AbstractCustomAction {

    @Override
    protected void initialize() throws Exception {
      Preconditions.checkArgument(true);
    }

    @Override
    public void run() { }
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
    public void initialize(WorkerContext context) throws Exception {
      Preconditions.checkArgument(true);
    }

    @Override
    public void run() { }
  }
}

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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;

/**
 *
 */
public class GoodWorkflowApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("GoodWorkflowApp");
    setDescription("WorkflowApp with multiple forks inside it");
    addMapReduce(new DummyMR());
    addWorkflow(new GoodWorkflow());
  }

  /**
   *
   */
  public class GoodWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("GoodWorkflow");
      setDescription("GoodWorkflow description");

      addAction(new DummyAction());

      // complex fork
      fork()
        .addMapReduce("DummyMR")
        .fork()
          .addAction(new DummyAction())
          .fork()
            .fork()
              .addMapReduce("DummyMR")
              .addAction(new DummyAction())
            .also()
              .addMapReduce("DummyMR")
            .join()
            .addMapReduce("DummyMR")
          .also()
            .addMapReduce("DummyMR")
          .join()
        .also()
          .addAction(new DummyAction())
        .join()
      .also()
        .addAction(new DummyAction())
      .join();

      addMapReduce("DummyMR");

      // simple fork
      fork()
        .addAction(new DummyAction())
      .also()
        .addMapReduce("DummyMR")
      .join();
    }
  }

  /**
   *
   */
  public class DummyMR extends AbstractMapReduce {
  }

  /**
   * DummyAction
   */
  public class DummyAction extends AbstractWorkflowAction {

    @Override
    public void run() {
    }
  }
}



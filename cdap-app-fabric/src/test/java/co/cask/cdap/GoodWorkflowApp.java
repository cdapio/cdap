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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowContext;

import javax.annotation.Nullable;

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
    addWorkflow(new AnotherGoodWorkflow());
  }

  /**
   *
   */
  public class GoodWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("GoodWorkflow");
      setDescription("GoodWorkflow description");

      addAction("a1", new DummyAction());

      // complex fork
      fork()
        .addMapReduce("mr1", "DummyMR")
        .fork()
          .addAction("a2", new DummyAction())
          .fork()
            .fork()
              .addMapReduce("mr2", "DummyMR")
              .addAction("a3", new DummyAction())
            .also()
              .addMapReduce("mr3", "DummyMR")
            .join()
            .addMapReduce("mr4", "DummyMR")
          .also()
            .addMapReduce("mr5", "DummyMR")
          .join()
        .also()
          .addAction("a4", new DummyAction())
        .join()
      .also()
        .addAction("a5", new DummyAction())
      .join();

      addMapReduce("mr6", "DummyMR");

      // simple fork
      fork()
        .addAction("a6", new DummyAction())
      .also()
        .addMapReduce("mr7", "DummyMR")
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

  public class AnotherGoodWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      addMapReduce("MR1");

      fork()
        .addMapReduce("MR2")
        .condition(new MyVerificationPredicate())
          .addMapReduce("MR3")
          .addMapReduce("MR4")
        .otherwise()
          .addMapReduce("MR5")
          .addMapReduce("MR6")
        .end()
        .addMapReduce("MR7")
      .also()
        .addMapReduce("MR8")
      .join();

     condition("anotherMyVerificationPredicate", new MyVerificationPredicate())
       .addSpark("SP1")
       .addSpark("SP2")
     .otherwise()
       .addSpark("SP3")
       .addSpark("SP4")
       .fork()
        .addSpark("SP5")
       .also()
        .addSpark("SP6")
       .join()
     .end();

     addSpark("SP7");
    }
  }

  public static final class MyVerificationPredicate implements Predicate<WorkflowContext> {

    @Override
    public boolean apply(@Nullable WorkflowContext input) {
      return false;
    }
  }
}



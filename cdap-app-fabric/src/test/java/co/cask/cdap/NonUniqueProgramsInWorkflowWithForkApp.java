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
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;

import javax.annotation.Nullable;

/**
 * App to test the non unique conditions in the Workflow fork.
 */
public class NonUniqueProgramsInWorkflowWithForkApp extends AbstractApplication {
  @Override
  public void configure() {
    addMapReduce(new NoOpMR());
    addWorkflow(new NonUniqueProgramsInWorkflowWithFork());
  }

  /**
   * No operation MapReduce program.
   */
  public static class NoOpMR extends AbstractMapReduce {
  }

  /**
   * Workflow program with two condition nodes added with the same predicate {@link MyTestPredicate}.
   */
  public static class NonUniqueProgramsInWorkflowWithFork extends AbstractWorkflow {

    @Override
    protected void configure() {
      fork()
        .condition(new MyTestPredicate())
          .addAction(new MyDummyAction())
        .otherwise()
          .condition(new MyTestPredicate())
            .addMapReduce("NoOpMR")
          .end()
        .end()
      .join();
    }
  }

  /**
   * Sample {@link Predicate} to test the unique names in the Workflow.
   */
  public static final class MyTestPredicate implements Predicate<WorkflowContext> {

    @Override
    public boolean apply(@Nullable WorkflowContext input) {
      return false;
    }
  }

  /**
   * Sample action added to the Workflow.
   */
  public static final class MyDummyAction extends AbstractCustomAction {

    @Override
    public void run() {

    }
  }
}

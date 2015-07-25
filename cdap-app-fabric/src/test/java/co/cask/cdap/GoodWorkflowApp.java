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
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
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
    for (int i = 1; i <= 7; i++) {
      addMapReduce(new DummyMR("MR" + String.valueOf(i)));
    }

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

      addAction(new DummyAction("DA1"));

      // complex fork
      fork()
        .addMapReduce("MR1")
        .fork()
          .addAction(new DummyAction("DA2"))
          .fork()
            .fork()
              .addMapReduce("MR2")
              .addAction(new DummyAction("DA3"))
            .also()
              .addMapReduce("MR3")
            .join()
            .addMapReduce("MR4")
          .also()
            .addMapReduce("MR5")
          .join()
        .also()
          .addAction(new DummyAction("DA4"))
        .join()
      .also()
        .addAction(new DummyAction("DA5"))
      .join();

      addMapReduce("MR6");

      // simple fork
      fork()
        .addAction(new DummyAction("DA6"))
      .also()
        .addMapReduce("MR7")
      .join();
    }
  }

  /**
   * Dummy MapReduce program
   */
  public class DummyMR extends AbstractMapReduce {
    private final String name;
    public DummyMR(String name) {
      this.name = name;
    }

    @Override
    protected void configure() {
      setName(name);
    }
  }

  /**
   * DummyAction
   */
  public class DummyAction extends AbstractWorkflowAction {
    private final String name;
    public DummyAction(String name) {
      this.name = name;
    }

    @Override
    public WorkflowActionSpecification configure() {
      return WorkflowActionSpecification.Builder.with()
        .setName(name)
        .setDescription(getDescription())
        .build();
    }

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

     condition(new AnotherVerificationPredicate())
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

  public static final class AnotherVerificationPredicate implements Predicate<WorkflowContext> {

    @Override
    public boolean apply(@Nullable WorkflowContext input) {
      return false;
    }
  }
}



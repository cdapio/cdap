/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.customaction.CustomAction;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.workflow.AbstractWorkflow;

public class DistributedWorkflowTestApp extends AbstractApplication<Config> {

  @Override
  public void configure() {
    addMapReduce(new TestMapReduce("mr1"));
    addMapReduce(new TestMapReduce("mr2"));
    addMapReduce(new TestMapReduce("mr3"));
    addMapReduce(new TestMapReduce("mr4"));

    addSpark(new TestSpark("s1"));
    addSpark(new TestSpark("s2"));
    addSpark(new TestSpark("s3"));
    addSpark(new TestSpark("s4"));

    addWorkflow(new ActionOnlyWorkflow());
    addWorkflow(new SequentialWorkflow());
    addWorkflow(new ComplexWorkflow());
  }

  /**
   * A workflow that only contains {@link CustomAction}.
   */
  public static final class ActionOnlyWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      addAction(new NoopAction());
    }

    public static final class NoopAction extends AbstractCustomAction {

      @Override
      public void run() throws Exception {
        // no-op
      }
    }
  }

  /**
   * A workflow that executes jobs sequentially.
   */
  public static final class SequentialWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      addMapReduce("mr1");
      addSpark("s1");
    }
  }

  /**
   * A workflow that has complex structure, including fork-join and condition.
   */
  public static final class ComplexWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      // The workflow has the following structure
      //
      //              |- mr3 -|  |-(if)-   s4  -|
      //  mr1 -> mr2 -         --                -- (end)
      //              |- s1  -|  |-(else)- mr4 -|
      //              |- s2  -|
      //              |- s3  -|

      addMapReduce("mr1");
      addMapReduce("mr2");
      fork()
        .addMapReduce("mr3")
      .also()
        .fork()
          .addSpark("s1")
        .also()
          .addSpark("s2")
        .also()
          .addSpark("s3")
        .join()
      .join();

      condition(input -> true)
        .addSpark("s4")
      .otherwise()
        .addMapReduce("mr4")
      .end();
    }
  }

  /**
   * MapReduce for the {@link DistributedWorkflowProgramRunnerTest}.
   */
  public static final class TestMapReduce extends AbstractMapReduce {

    private final String name;

    TestMapReduce(String name) {
      this.name = name;
    }

    @Override
    protected void configure() {
      setName(name);
    }
  }

  /**
   * Spark for the {@link DistributedWorkflowProgramRunnerTest}.
   */
  public static final class TestSpark extends AbstractSpark {

    private final String name;

    TestSpark(String name) {
      this.name = name;
    }

    @Override
    protected void configure() {
      setName(name);
    }
  }
}

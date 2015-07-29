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
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;

/**
 * App to test the token persistence after FORK in the Workflow.
 */
public class WorkflowTokenPersistenceTestApp extends AbstractApplication {
  public static final String NAME = "WorkflowTokenPersistenceTestApp";

  @Override
  public void configure() {
    setName(NAME);
    addWorkflow(new TokenPersistenceWorkflow());
  }

  /**
   * Workflow to test the persistence of the token after fork.
   */
  public static class TokenPersistenceWorkflow extends AbstractWorkflow {
    public static final String NAME = "TokenPersistenceWorkflow";
    @Override
    protected void configure() {
      setName(NAME);

      fork()
        .addAction(new SomeAction("oneAction"))
      .also()
        .addAction(new SomeAction("anotherAction"))
      .join();
    }
  }

  /**
   * Action to test the Workflow.
   */
  static final class SomeAction extends AbstractWorkflowAction {
    private final String name;
    public SomeAction(String name) {
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
      getContext().getToken().put("some.key", "some.value");
    }
  }
}

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

/**
 *
 */
public class OneActionWorkflowApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("OneActionWorkflowApp");
    setDescription("Workflow with a single action");
    addWorkflow(new OneActionWorkflow());
  }

  /**
   *
   */
  private static class OneActionWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
        setName("OneActionWorkflow");
        setDescription("One action workflow");
        addAction(new EmptyAction());
    }
  }

  /**
   *
   */
  private static class EmptyAction extends AbstractWorkflowAction {
    @Override
    public void run() {
    }
  }
}

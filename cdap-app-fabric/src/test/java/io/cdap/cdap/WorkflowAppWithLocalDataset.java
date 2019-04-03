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
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.workflow.AbstractWorkflow;

/**
 * App with local dataset.
 */
public class WorkflowAppWithLocalDataset extends AbstractApplication {
  public static final String APP_NAME = "WorkflowAppWithLocalDataset";
  public static final String WORKFLOW_NAME = "WorkflowWithLocalDataset";
  @Override
  public void configure() {
    addWorkflow(new WorkflowWithLocalDataset());
  }

  /**
   * Workflow with local dataset.
   */
  public static class WorkflowWithLocalDataset extends AbstractWorkflow {

    @Override
    protected void configure() {
      createLocalDataset("testdataset", Table.class);
      addAction(new MyCustomAction());
    }
  }

  /**
   * Custom action in the Workflow which does nothing
   */
  public static class MyCustomAction extends AbstractCustomAction {

    @Override
    public void run() throws Exception {
      // no-op
    }
  }
}

/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Workflow app for testing the deletion handler for the local datasets.
 */
public class WorkflowAppWithLocalDatasets extends AbstractApplication {
  public static final String NAME = "WorkflowAppWithLocalDatasets";
  public static final String WORKFLOW_NAME = "WorkflowWithLocalDatasets";
  public static final String TABLE_DATASET = "MyTable";
  public static final String FILE_DATASET = "MyFile";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Application to test the deletion of the local datasets.");
    addWorkflow(new WorkflowWithLocalDatasets());
  }

  /**
   * Workflow creating the local datasets.
   */
  public static class WorkflowWithLocalDatasets extends AbstractWorkflow {

    @Override
    protected void configure() {
      setName(WORKFLOW_NAME);
      setDescription("Workflow which creates the local datasets.");
      createLocalDataset(TABLE_DATASET, KeyValueTable.class, DatasetProperties.builder().add("foo", "bar").build());
      createLocalDataset(FILE_DATASET, FileSet.class,
                         DatasetProperties.builder().add("anotherFoo", "anotherBar").build());
      addAction(new MyWorkflowAction());
    }
  }

  /**
   * Workflow action for co-ordination.
   */
  public static class MyWorkflowAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(MyWorkflowAction.class);
    @Override
    public void run() {
      Map<String, String> runtimeArguments = getContext().getRuntimeArguments();
      try {
        File waitFile = new File(runtimeArguments.get("wait.file"));
        waitFile.createNewFile();

        File doneFile = new File(runtimeArguments.get("done.file"));
        while (!doneFile.exists()) {
          TimeUnit.MILLISECONDS.sleep(50);
        }

      } catch (Exception e) {
        LOG.error("Error occurred while executing action", e);
      }
    }
  }
}

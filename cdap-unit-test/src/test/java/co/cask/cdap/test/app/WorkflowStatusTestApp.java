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

package co.cask.cdap.test.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Application to test the status of the {@link Workflow}.
 */
public class WorkflowStatusTestApp extends AbstractApplication {
  public static final String APP_NAME = "WorkflowStatusTest";
  public static final String WORKFLOW_NAME = "MyWorkflow";
  public static final String ACTION_NAME = "MyAction";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("Application to test the status of Workflow.");
    addWorkflow(new MyWorkflow());
  }

  /**
   * Workflow to test the status.
   */
  public static class MyWorkflow extends AbstractWorkflow {
    private static final Logger LOG = LoggerFactory.getLogger(MyWorkflow.class);

    @Override
    protected void configure() {
      setName(WORKFLOW_NAME);
      setDescription("Workflow to test the status.");
      addAction(new MyAction());
    }

    @Override
    public void destroy() {
      if (getContext().isSuccessful()) {
        File successFile = new File(getContext().getRuntimeArguments().get("workflow.success.file"));
        try {
          successFile.createNewFile();
        } catch (IOException e) {
          LOG.error("Error occurred while creating file {}", successFile.getAbsolutePath(), e);
        }
      }
    }
  }

  /**
   * Custom action to test the status.
   */
  public static class MyAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(MyAction.class);

    @Override
    public void run() {
      if (getContext().getRuntimeArguments().containsKey("throw.exception")) {
        throw new RuntimeException("Exception is thrown");
      }
    }

    @Override
    public void destroy() {
      if (getContext().isSuccessful()) {
        File successFile = new File(getContext().getRuntimeArguments().get("action.success.file"));
        try {
          successFile.createNewFile();
        } catch (IOException e) {
          LOG.error("Error occurred while creating file {}", successFile.getAbsolutePath(), e);
        }
      }
    }
  }
}


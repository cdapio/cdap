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
import co.cask.cdap.api.workflow.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class WorkflowAppWithFork extends AbstractApplication {
  @Override
  public void configure() {
    setName("WorkflowAppWithFork");
    setDescription("Workflow App containing fork");
    addWorkflow(new WorkflowWithFork());
  }

  /**
   *
   */
  public static class WorkflowWithFork extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("WorkflowWithFork");
      setDescription("WorkflowWithFork description");
      fork().addAction(new OneAction()).also().addAction(new AnotherAction()).join();
    }
  }

  /**
   *
   */
  static class OneAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(OneAction.class);
    private String filePath = "";
    private String doneFilePath = "";

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      filePath = context.getRuntimeArguments().get("oneaction.file");
      doneFilePath = context.getRuntimeArguments().get("done.file");
    }

    @Override
    public void run() {
      LOG.info("Ran one action");
      try {
        File file = new File(filePath);
        file.createNewFile();
        File doneFile = new File(doneFilePath);
        while (!doneFile.exists()) {
          TimeUnit.SECONDS.sleep(1);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }

  /**
   *
   */
  static class AnotherAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(AnotherAction.class);
    private String filePath = "";
    private String doneFilePath = "";

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      filePath = context.getRuntimeArguments().get("anotheraction.file");
      doneFilePath = context.getRuntimeArguments().get("done.file");
    }

    @Override
    public void run() {
      LOG.info("Ran another action");
      try {
        File file = new File(filePath);
        file.createNewFile();
        File doneFile = new File(doneFilePath);
        while (!doneFile.exists()) {
          TimeUnit.SECONDS.sleep(1);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }
}

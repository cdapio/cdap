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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class WorkflowAppWithFork extends AbstractApplication {
  public static final String SYNCH_ON_FILE = "/tmp/WorkflowAppWithFork.done";
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
    @Override
    public void run() {
      LOG.info("Ran one action");
      File doneFile = new File(SYNCH_ON_FILE);
      while (!doneFile.exists()) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          // no-op
        }
      }
    }
  }

  /**
   *
   */
  static class AnotherAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(AnotherAction.class);
    @Override
    public void run() {
      LOG.info("Ran another action");
      File doneFile = new File(SYNCH_ON_FILE);
      while (!doneFile.exists()) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          // no-op
        }
      }
    }
  }
}

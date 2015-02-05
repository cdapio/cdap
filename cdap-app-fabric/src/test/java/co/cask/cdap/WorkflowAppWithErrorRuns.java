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

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class WorkflowAppWithErrorRuns extends AbstractApplication {

  @Override
  public void configure() {
    setName("WorkflowAppWithErrorRuns");
    setDescription("Sample Workflow application with some error runs.");
    addWorkflow(new WorkflowWithErrorRuns());
    scheduleWorkflow("SampleSchedule", "0/1 * * * * ?", "WorkflowWithErrorRuns");
  }

  /**
   * Sample Workflow which throws exception based on the runtime arguments.
   */
  public static class WorkflowWithErrorRuns extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("WorkflowWithErrorRuns");
      setDescription("Sample Workflow which throws exception based on the runtime arguments.");
      addAction(new DummyAction());
    }
  }

  /**
   * DummyAction
   */
  public static class DummyAction extends AbstractWorkflowAction {
    private static final Logger LOG = LoggerFactory.getLogger(DummyAction.class);
    @Override
    public void run() {
      LOG.info("Ran dummy action");
      if (getContext().getRuntimeArguments().containsKey("ThrowError")) {
        throw new RuntimeException("Error");
      }
      try {
        TimeUnit.MILLISECONDS.sleep(500);
      } catch (InterruptedException e) {
        LOG.info("Interrupted");
      }
    }
  }
}

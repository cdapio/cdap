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
public class WorkflowSchedulesWithSameNameApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("WorkflowSchedulesWithSameNameApp");
    setDescription("Application with Workflow containing multiple schedules with the same name");
    addWorkflow(new WorkflowSchedulesWithSameName());
    scheduleWorkflow("DailySchedule", "0 4 * * *", "WorkflowSchedulesWithSameName");
    // configuring Workflow with the same schedule name again should fail
    scheduleWorkflow("DailySchedule", "0 5 * * *", "WorkflowSchedulesWithSameName");
  }

  /**
   *
   */
  private static class WorkflowSchedulesWithSameName extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("WorkflowSchedulesWithSameName");
      setDescription("Workflow configured with multiple schedules with the same name");
      addAction(new DummyAction());
    }
  }

  /**
   * DummyAction
   */
  public static class DummyAction extends AbstractWorkflowAction {
    @Override
    public void run() {
      // no-op
    }
  }
}

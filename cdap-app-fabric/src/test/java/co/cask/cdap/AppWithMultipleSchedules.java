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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AppWithMultipleSchedules extends AbstractApplication {
  public static final String NAME = "AppWithMultipleScheduledWorkflows";
  public static final String SOME_WORKFLOW = "SomeWorkflow";
  public static final String ANOTHER_WORKFLOW = "AnotherWorkflow";
  public static final String TRIGGERED_WORKFLOW = "TriggeredWorkflow";
  public static final String WORKFLOW_COMPLETED_SCHEDULE = "WorkflowCompletedSchedule";

  @Override
  public void configure() {
    setName("AppWithMultipleScheduledWorkflows");
    setDescription("Sample application with multiple Workflows");
    addWorkflow(new SomeWorkflow());
    addWorkflow(new AnotherWorkflow());
    addWorkflow(new TriggeredWorkflow());
    schedule(buildSchedule("SomeSchedule1", ProgramType.WORKFLOW, SOME_WORKFLOW).triggerByTime("0 4 * * *"));
    schedule(buildSchedule("SomeSchedule2", ProgramType.WORKFLOW, SOME_WORKFLOW).triggerByTime("0 5 * * *"));
    schedule(buildSchedule("AnotherSchedule1", ProgramType.WORKFLOW, ANOTHER_WORKFLOW).triggerByTime("0 6 * * *"));
    schedule(buildSchedule("AnotherSchedule2", ProgramType.WORKFLOW, ANOTHER_WORKFLOW).triggerByTime("0 7 * * *"));
    schedule(buildSchedule("AnotherSchedule3", ProgramType.WORKFLOW, ANOTHER_WORKFLOW).triggerByTime("0 8 * * *"));
    schedule(buildSchedule("TriggeredWorkflowSchedule", ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerByTime("0 8 * * *"));
    schedule(buildSchedule("WorkflowCompletedSchedule1", ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerOnProgramStatus(ProgramType.WORKFLOW, SOME_WORKFLOW, ProgramStatus.COMPLETED));
    schedule(buildSchedule("WorkflowFailedSchedule", ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerOnProgramStatus(ProgramType.WORKFLOW, SOME_WORKFLOW, ProgramStatus.FAILED));
    schedule(buildSchedule("WorkflowCompletedFailedSchedule", ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerOnProgramStatus(ProgramType.WORKFLOW, SOME_WORKFLOW,
                                       ProgramStatus.COMPLETED, ProgramStatus.FAILED));
    schedule(buildSchedule(WORKFLOW_COMPLETED_SCHEDULE, ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerOnProgramStatus(ProgramType.WORKFLOW, ANOTHER_WORKFLOW, ProgramStatus.COMPLETED));
  }

  /**
   * Some Workflow
   */
  public static class SomeWorkflow extends AbstractWorkflow {
    public static final String NAME = SOME_WORKFLOW;
    @Override
    public void configure() {
      setName(NAME);
      setDescription("SomeWorkflow description");
      addAction(new SomeDummyAction());
    }
  }

  /**
   * Some Dummy Action
   */
  public static class SomeDummyAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(SomeDummyAction.class);

    @Override
    public void run() {
      LOG.info("Ran some dummy action");
    }
  }

  /**
   * Another Workflow
   */
  public static class AnotherWorkflow extends AbstractWorkflow {
    public static final String NAME = ANOTHER_WORKFLOW;
    @Override
    public void configure() {
      setName(NAME);
      setDescription("AnotherWorkflow description");
      addAction(new DummyWorkflowTokenAction());
    }
  }

  /**
   * Another Dummy Action
   */
  public static class DummyWorkflowTokenAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(DummyWorkflowTokenAction.class);
    public static final String DUMMY_KEY = "dummy.key";
    public static final String DUMMY_VALUE = "dummy.value";

    @Override
    public void run() {
      LOG.info("Ran a dummy workflow token action");
      getContext().getWorkflowToken().put(DUMMY_KEY, DUMMY_VALUE);
    }
  }

  /**
   * Triggered Workflow
   */
  public static class TriggeredWorkflow extends AbstractWorkflow {
    public static final String NAME = TRIGGERED_WORKFLOW;
    @Override
    public void configure() {
      setName(NAME);
      setDescription("TriggeredWorkflow description");
      addAction(new SomeDummyAction());
    }
  }
}

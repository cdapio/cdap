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
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AppWithMultipleScheduledWorkflows extends AbstractApplication {

  @Override
  public void configure() {
    setName("AppWithMultipleScheduledWorkflows");
    setDescription("Sample application with multiple Workflows");
    addWorkflow(new SomeWorkflow());
    addWorkflow(new AnotherWorkflow());
    schedule(buildSchedule("SomeSchedule1", ProgramType.WORKFLOW, "SomeWorkflow").triggerByTime("0 4 * * *"));
    schedule(buildSchedule("SomeSchedule2", ProgramType.WORKFLOW, "SomeWorkflow").triggerByTime("0 5 * * *"));
    schedule(buildSchedule("AnotherSchedule1", ProgramType.WORKFLOW, "AnotherWorkflow").triggerByTime("0 6 * * *"));
    schedule(buildSchedule("AnotherSchedule2", ProgramType.WORKFLOW, "AnotherWorkflow").triggerByTime("0 7 * * *"));
    schedule(buildSchedule("AnotherSchedule3", ProgramType.WORKFLOW, "AnotherWorkflow").triggerByTime("0 8 * * *"));
  }

  /**
   * Some Workflow
   */
  public static class SomeWorkflow extends AbstractWorkflow {
    public static final String NAME = "SomeWorkflow";
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
    public static final String NAME = "AnotherWorkflow";
    @Override
    public void configure() {
      setName(NAME);
      setDescription("AnotherWorkflow description");
      addAction(new AnotherDummyAction());
    }
  }

  /**
   * Another Dummy Action
   */
  public static class AnotherDummyAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(AnotherDummyAction.class);

    @Override
    public void run() {
      LOG.info("Ran another dummy action");
    }
  }
}



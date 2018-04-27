/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Simple workflow, that sleeps inside a CustomAction, This class is used for testing the workflow status.
 */
public class NoOpWorkflowApp extends AbstractApplication {
  public static final String NO_OP_WORKFLOW = "NoOpWorkflow";
  public static final String TRIGGERED_WORKFLOW = "TriggeredWorkflow";


  @Override
  public void configure() {
    setName("NoOpWorkflowApp");
    setDescription("NoOpWorkflowApp");
    addWorkflow(new NoOpWorkflow());
    addWorkflow(new TriggeredWorkflow());
    schedule(buildSchedule("sched", ProgramType.WORKFLOW, TRIGGERED_WORKFLOW)
               .triggerOnProgramStatus(ProgramType.WORKFLOW, NO_OP_WORKFLOW, ProgramStatus.COMPLETED,
                                       ProgramStatus.FAILED, ProgramStatus.KILLED));
  }

  /**
   *
   */
  public static class NoOpWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName(NO_OP_WORKFLOW);
      setDescription("FunWorkflow description");
      addAction(new CustomAction(NO_OP_WORKFLOW));
    }
  }

  /**
   *
   */
  public static class TriggeredWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName(TRIGGERED_WORKFLOW);
      setDescription("FunWorkflow description");
      addAction(new CustomAction(TRIGGERED_WORKFLOW));
    }
  }

  /**
   *
   */
  public static final class CustomAction extends AbstractCustomAction {

    private static final Logger LOG = LoggerFactory.getLogger(co.cask.cdap.SleepingWorkflowApp.CustomAction.class);

    private final String name;

    @Property
    private final boolean condition = true;

    public CustomAction(String name) {
      this.name = name;
    }

    @Override
    public void configure() {
      setName(name);
      setDescription(name);
    }

    @Override
    public void initialize() throws Exception {
      LOG.info("Custom action initialized: " + getContext().getSpecification().getName());
    }

    @Override
    public void destroy() {
      super.destroy();
      LOG.info("Custom action destroyed: " + getContext().getSpecification().getName());
    }

    @Override
    public void run() {
      LOG.info("Custom run completed.");
    }
  }

}

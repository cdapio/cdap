/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.internal.workflow;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import co.cask.cdap.api.workflow.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Action to be executed in Workflow for Programs.
 * {@link ProgramWorkflowAction#run} does a call on {@link Callable} of {@link RuntimeContext}.
 */
public final class ProgramWorkflowAction extends AbstractWorkflowAction {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramWorkflowAction.class);
  private static final String PROGRAM_NAME = "ProgramName";
  public static final String PROGRAM_TYPE = "ProgramType";

  private String programName;
  private SchedulableProgramType programType;

  public ProgramWorkflowAction(String programName, SchedulableProgramType programType) {
    this.programName = programName;
    this.programType = programType;
  }

  @Override
  public void configure() {
    setName(programName);
    setDescription("Workflow action for " + programType.name() + " " + programName);

    Map<String, String> properties = new HashMap<>();
    properties.put(PROGRAM_TYPE, programType.name());
    properties.put(PROGRAM_NAME, programName);
    setProperties(properties);
  }

  @Override
  public void run() {
    try {
      String programName = getContext().getSpecification().getProperties().get(PROGRAM_NAME);
      Runnable programRunner = getContext().getProgramRunner(programName);
      LOG.info("Starting Program for workflow action: {}", programName);
      programRunner.run();

      // TODO (terence) : Put something back to context.

      LOG.info("{} Program {} workflow action completed",
               programType != null ? programType.name() : null, programName);
    } catch (Exception e) {
      LOG.info("Failed to execute {} Program {} in workflow", programType, programName, e);
      throw e;
    }
  }
}

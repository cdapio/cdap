/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.workflow.WorkflowActionConfigurer;
import co.cask.cdap.api.workflow.WorkflowContext;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private Runnable programRunner;
  private SchedulableProgramType programType;

  public ProgramWorkflowAction(String programName, SchedulableProgramType programType) {
    this.programName = programName;
    this.programType = programType;
  }

  @Override
  public void configure(WorkflowActionConfigurer configurer) {
    super.configure(configurer);
    setName(programName);
    setDescription("Workflow action for " + programType.name() + " " + programName);
    setProperties(ImmutableMap.of(PROGRAM_TYPE, programType.name(),
                                  PROGRAM_NAME, programName));
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    programName = context.getSpecification().getProperties().get(PROGRAM_NAME);
    if (programName == null) {
      throw new IllegalArgumentException("No Program name provided.");
    }

    programRunner = context.getProgramRunner(programName);
    programType = context.getSpecification().getProperties().containsKey(PROGRAM_TYPE) ?
      SchedulableProgramType.valueOf(context.getSpecification().getProperties().get(PROGRAM_TYPE)) : null;

    LOG.info("Initialized for {} Program {} in workflow action",
             programType != null ? programType.name() : null, programName);
  }

  @Override
  public void run() {
    try {
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

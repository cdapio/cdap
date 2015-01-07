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
import co.cask.cdap.api.schedule.SchedulableProgram;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Action to be executed in Workflow for Programs.
 * {@link ProgramWorkflowAction#run} does a call on {@link Callable} of {@link RuntimeContext}.
 */
public final class ProgramWorkflowAction implements WorkflowAction {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramWorkflowAction.class);
  private static final String PROGRAM_NAME = "ProgramName";
  public static final String PROGRAM_TYPE = "ProgramType";

  private final String name;
  private String programName;
  private Callable<RuntimeContext> programRunner;
  private SchedulableProgram programType;
  private WorkflowContext context;

  public ProgramWorkflowAction(String name, String programName, SchedulableProgram programType) {
    this.name = name;
    this.programName = programName;
    this.programType = programType;
  }

  @Override
  public WorkflowActionSpecification configure() {
    return WorkflowActionSpecification.Builder.with()
      .setName(name)
      .setDescription("Workflow action for " + programName)
      .withOptions(ImmutableMap.of(
        PROGRAM_TYPE, programType.name(),
        PROGRAM_NAME, programName
      ))
      .build();
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    this.context = context;

    programName = context.getSpecification().getProperties().get(PROGRAM_NAME);
    Preconditions.checkNotNull(programName, "No Program name provided.");

    programRunner = context.getProgramRunner(programName);
    programType = context.getRuntimeArguments().containsKey(PROGRAM_TYPE) ? 
                              SchedulableProgram.valueOf(context.getRuntimeArguments().get(PROGRAM_TYPE)) : null;

    LOG.info("Initialized for {} Program {} in workflow action",
             programType != null ? programType.name() : null, programName);
  }

  @Override
  public void run() {
    try {
      LOG.info("Starting Program for workflow action: {}", programName);
      RuntimeContext runtimeContext = programRunner.call();

      // TODO (terence) : Put something back to context.

      LOG.info("{} Program {} workflow action completed",
               programType != null ? programType.name() : null, programName);
    } catch (Exception e) {
      LOG.info("Failed to execute {} Program {} in workflow: {}", 
               programType != null ? programType.name() : null, programName, e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void destroy() {
    // No-op
  }
}

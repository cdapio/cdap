/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.internal.RunIds;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class WorkflowProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowProgramRunner.class);

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify options
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getProcessorType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(program.getProgramName());
    Preconditions.checkNotNull(workflowSpec, "Missing WorkflowSpecification for %s", program.getProgramName());

    // TODO
    RunId runId = RunIds.generate();
    WorkflowDriver driver = new WorkflowDriver(program, workflowSpec);
    driver.start();

    return new WorkflowProgramController(program.getProgramName(), driver, runId);
  }
}

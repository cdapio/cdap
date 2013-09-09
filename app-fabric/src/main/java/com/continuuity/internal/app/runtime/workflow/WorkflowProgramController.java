/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.weave.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
final class WorkflowProgramController extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowProgramController.class);

  private final WorkflowDriver driver;

  WorkflowProgramController(String programName, WorkflowDriver driver, RunId runId) {
    super(programName, runId);
    this.driver = driver;
    started();
  }

  @Override
  protected void doSuspend() throws Exception {
    LOG.info("Suspend not supported.");
  }

  @Override
  protected void doResume() throws Exception {
    LOG.info("Resume not supported.");
  }

  @Override
  protected void doStop() throws Exception {
    driver.stopAndWait();
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    LOG.info("Command ignored {}, {}", name, value);
  }
}

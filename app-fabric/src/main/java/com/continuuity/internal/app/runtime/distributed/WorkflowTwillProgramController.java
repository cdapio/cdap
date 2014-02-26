/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
final class WorkflowTwillProgramController extends AbstractTwillProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowTwillProgramController.class);

  WorkflowTwillProgramController(String programName, TwillController controller) {
    super(programName, controller);
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    // Workflow doesn't have any command for now.
    LOG.info("Command ignored for workflow controller: {}, {}", name, value);
  }
}

/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.runtime.ProgramResourceReporter;
import com.continuuity.weave.api.WeaveController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
final class WorkflowWeaveProgramController extends AbstractWeaveProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowWeaveProgramController.class);

  WorkflowWeaveProgramController(String programName, WeaveController controller,
                                 ProgramResourceReporter resourceReporter) {
    super(programName, controller, resourceReporter);
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    // Workflow doesn't have any command for now.
    LOG.info("Command ignored for workflow controller: {}, {}", name, value);
  }
}

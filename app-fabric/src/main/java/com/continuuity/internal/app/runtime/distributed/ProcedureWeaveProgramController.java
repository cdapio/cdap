/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.weave.api.WeaveController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
*
*/
final class ProcedureWeaveProgramController extends AbstractWeaveProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureWeaveProgramController.class);

  ProcedureWeaveProgramController(String programName, WeaveController controller) {
    super(programName, controller);
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {

    if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Map)) {
      return;
    }

    Map<String, Integer> command = (Map<String, Integer>) value;
    try {
      for (Map.Entry<String, Integer> entry : command.entrySet()) {
        LOG.info("Change procedure instance count: {} new count is: {}",  entry.getKey(), entry.getValue());
        weaveController.changeInstances(entry.getKey(), entry.getValue());
        LOG.info("Procedure instance count changed: {} new count is {}", entry.getKey(), entry.getValue());
      }
    } catch (Throwable t) {
      LOG.error(String.format("Fail to change instances: %s", command), t);
    }
  }
}

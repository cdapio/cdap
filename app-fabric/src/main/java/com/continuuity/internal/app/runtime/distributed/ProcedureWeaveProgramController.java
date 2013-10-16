/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.weave.api.WeaveController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
*
*/
final class ProcedureWeaveProgramController extends AbstractWeaveProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureWeaveProgramController.class);
  private final WeaveController controller;
  private final Lock lock;

  ProcedureWeaveProgramController(String programName, WeaveController controller) {
    super(programName, controller);
    this.controller = controller;
    this.lock = new ReentrantLock();
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {

    if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Map)) {
      return;
    }

    Map<String, Integer> command = (Map<String, Integer>) value;
    lock.lock();
    try {
      for (Map.Entry<String, Integer> entry : command.entrySet()) {
        LOG.info("Change procedure instance count: " + entry.getKey() + " new count is " + entry.getValue());
        controller.changeInstances(entry.getKey(), entry.getValue());
        LOG.info("Procedure instance count changed: " + entry.getKey() + " new count is " + entry.getValue());
      }
    } catch (Throwable t) {
      LOG.error(String.format("Fail to change instances: %s", command), t);
    } finally {
      lock.unlock();
    }
  }
}

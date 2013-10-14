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
 * A ProgramController for flow program that are launched through Weave.
 */
final class FlowWeaveProgramController extends AbstractWeaveProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(FlowWeaveProgramController.class);

  private final Lock lock;
  private final DistributedFlowletInstanceUpdater instanceUpdater;

  FlowWeaveProgramController(String programId, WeaveController controller,
                             DistributedFlowletInstanceUpdater instanceUpdater) {
    super(programId, controller);
    this.lock = new ReentrantLock();
    this.instanceUpdater = instanceUpdater;
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
        changeInstances(entry.getKey(), entry.getValue());
      }
    } catch (Throwable t) {
      LOG.error(String.format("Fail to change instances: %s", command), t);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Change the number of instances of the running flowlet. Notice that this method needs to be
   * synchronized as change of instances involves multiple steps that need to be completed all at once.
   * @param flowletId Name of the flowlet
   * @param newInstanceCount New instance count
   * @throws java.util.concurrent.ExecutionException
   * @throws InterruptedException
   */
  private synchronized void changeInstances(String flowletId, int newInstanceCount) throws Exception {
    instanceUpdater.update(flowletId, newInstanceCount);
  }
}

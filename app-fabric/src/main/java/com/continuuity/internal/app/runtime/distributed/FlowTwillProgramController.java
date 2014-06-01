/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A ProgramController for flow program that are launched through Twill.
 */
final class FlowTwillProgramController extends AbstractTwillProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(FlowTwillProgramController.class);

  private final Lock lock;
  private final DistributedFlowletInstanceUpdater instanceUpdater;

  FlowTwillProgramController(String programId, TwillController controller,
                             DistributedFlowletInstanceUpdater instanceUpdater) {
    super(programId, controller);
    this.lock = new ReentrantLock();
    this.instanceUpdater = instanceUpdater;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doCommand(String name, Object value) throws Exception {
    if (!ProgramOptionConstants.FLOWLET_INSTANCES.equals(name) || !(value instanceof Map)) {
      return;
    }
    Map<String, String> command = (Map<String, String>) value;
    lock.lock();
    try {
      changeInstances(command.get("flowlet"),
                      Integer.valueOf(command.get("newInstances")),
                      Integer.valueOf(command.get("oldInstances")));
    } catch (Throwable t) {
      LOG.error(String.format("Fail to change instances: %s", command), t);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Change the number of instances of the running flowlet. Notice that this method needs to be
   * synchronized as change of instances involves multiple steps that need to be completed all at once.
   * @param flowletId Name of the flowlet.
   * @param newInstanceCount New instance count.
   * @param oldInstanceCount Old instance count.
   * @throws java.util.concurrent.ExecutionException
   * @throws InterruptedException
   */
  private synchronized void changeInstances(String flowletId, int newInstanceCount, int oldInstanceCount)
    throws Exception {
    instanceUpdater.update(flowletId, newInstanceCount, oldInstanceCount);
  }
}

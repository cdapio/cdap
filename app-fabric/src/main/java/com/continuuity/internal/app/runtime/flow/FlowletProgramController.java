/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * A {@link com.continuuity.app.runtime.ProgramController} for controlling a running flowlet.
 */
final class FlowletProgramController extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletProgramController.class);

  private final BasicFlowletContext flowletContext;
  private final FlowletProcessDriver driver;
  private final Collection<QueueConsumerSupplier> queueConsumerSuppliers;

  FlowletProgramController(String programName, String flowletName,
                           BasicFlowletContext flowletContext, FlowletProcessDriver driver,
                           Collection<QueueConsumerSupplier> queueConsumerSuppliers) {
    super(programName + ":" + flowletName, flowletContext.getRunId());
    this.flowletContext = flowletContext;
    this.driver = driver;
    this.queueConsumerSuppliers = queueConsumerSuppliers;
    started();
  }

  @Override
  protected void doSuspend() throws Exception {
    LOG.info("Suspending flowlet: " + flowletContext);
    driver.suspend();
    // Close all consumers
    for (QueueConsumerSupplier queueConsumerSupplier : queueConsumerSuppliers) {
      queueConsumerSupplier.close();
    }
    LOG.info("Flowlet suspended: " + flowletContext);
  }

  @Override
  protected void doResume() throws Exception {
    LOG.info("Resuming flowlet: " + flowletContext);
    // Open consumers
    for (QueueConsumerSupplier queueConsumerSupplier : queueConsumerSuppliers) {
      queueConsumerSupplier.open(flowletContext.getInstanceCount());
    }
    driver.resume();
    LOG.info("Flowlet resumed: " + flowletContext);
  }

  @Override
  protected void doStop() throws Exception {
    LOG.info("Stopping flowlet: " + flowletContext);
    driver.stopAndWait();
    // Close all consumers
    for (QueueConsumerSupplier queueConsumerSupplier : queueConsumerSuppliers) {
      queueConsumerSupplier.close();
    }
    LOG.info("Flowlet stopped: " + flowletContext);
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    Preconditions.checkState(getState() == State.SUSPENDED,
                             "Cannot change instance count when flowlet is running.");
    if (!"instances".equals(name) || !(value instanceof Integer)) {
      return;
    }
    int instances = (Integer) value;
    LOG.info("Change flowlet instance count: " + flowletContext + ", new count is " + instances);
    changeInstanceCount(flowletContext, instances);
    LOG.info("Flowlet instance count changed: " + flowletContext + ", new count is " + instances);
  }

  private void changeInstanceCount(BasicFlowletContext flowletContext, int instanceCount) {
    Preconditions.checkState(getState() == State.SUSPENDED,
                             "Cannot change instance count of a flowlet without suspension.");
    flowletContext.setInstanceCount(instanceCount);
  }
}

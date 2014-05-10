/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
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
  private final Collection<ConsumerSupplier<?>> consumerSuppliers;

  /**
   * Constructs an instance. The instance must be constructed before the flowlet driver starts.
   */
  FlowletProgramController(String programName, String flowletName,
                           BasicFlowletContext flowletContext, FlowletProcessDriver driver,
                           Collection<ConsumerSupplier<?>> consumerSuppliers) {
    super(programName + ":" + flowletName, flowletContext.getRunId());
    this.flowletContext = flowletContext;
    this.driver = driver;
    this.consumerSuppliers = consumerSuppliers;
    listenDriveState(driver);
  }

  @Override
  protected void doSuspend() throws Exception {
    LOG.info("Suspending flowlet: " + flowletContext);
    driver.suspend();
    // Close all consumers
    for (ConsumerSupplier consumerSupplier : consumerSuppliers) {
      consumerSupplier.close();
    }
    LOG.info("Flowlet suspended: " + flowletContext);
  }

  @Override
  protected void doResume() throws Exception {
    LOG.info("Resuming flowlet: " + flowletContext);
    // Open consumers
    for (ConsumerSupplier consumerSupplier : consumerSuppliers) {
      consumerSupplier.open(flowletContext.getInstanceCount());
    }
    driver.resume();
    LOG.info("Flowlet resumed: " + flowletContext);
  }

  @Override
  protected void doStop() throws Exception {
    LOG.info("Stopping flowlet: " + flowletContext);
    driver.stopAndWait();
    // Close all consumers
    for (ConsumerSupplier consumerSupplier : consumerSuppliers) {
      Closeables.closeQuietly(consumerSupplier);
    }
    LOG.info("Flowlet stopped: " + flowletContext);
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    Preconditions.checkState(getState() == State.SUSPENDED,
                             "Cannot change instance count when flowlet is running.");
    if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Integer)) {
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

  private void listenDriveState(FlowletProcessDriver driver) {
    driver.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        started();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.error("Flowlet terminated with exception", failure);
        error(failure);
      }

      @Override
      public void terminated(Service.State from) {
        if (getState() != State.STOPPING) {
          LOG.warn("Flowlet terminated by itself");
          // Close all consumers
          for (ConsumerSupplier consumerSupplier : consumerSuppliers) {
            Closeables.closeQuietly(consumerSupplier);
          }
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }
}

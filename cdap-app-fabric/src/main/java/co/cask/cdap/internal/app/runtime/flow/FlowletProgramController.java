/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.ProgramState;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * A {@link co.cask.cdap.app.runtime.ProgramController} for controlling a running flowlet.
 */
final class FlowletProgramController extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletProgramController.class);

  private final BasicFlowletContext flowletContext;
  private final FlowletRuntimeService driver;
  private final Collection<ConsumerSupplier<?>> consumerSuppliers;

  /**
   * Constructs an instance. The instance must be constructed before the flowlet driver starts.
   */
  FlowletProgramController(String programName, String flowletName,
                           BasicFlowletContext flowletContext, FlowletRuntimeService driver,
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
    try {
      driver.stopAndWait();
    } finally {
      // Close all consumers
      for (ConsumerSupplier consumerSupplier : consumerSuppliers) {
        Closeables.closeQuietly(consumerSupplier);
      }
      flowletContext.close();
    }
    LOG.info("Flowlet stopped: " + flowletContext);
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    Preconditions.checkState(getState() == ProgramState.SUSPENDED,
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
    Preconditions.checkState(getState() == ProgramState.SUSPENDED,
                             "Cannot change instance count of a flowlet without suspension.");
    flowletContext.setInstanceCount(instanceCount);
  }

  private void listenDriveState(FlowletRuntimeService driver) {
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
        if (getState() != ProgramState.STOPPING) {
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

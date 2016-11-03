/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * For updating number of flowlet instances
 */
@NotThreadSafe
final class DistributedFlowletInstanceUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedFlowletInstanceUpdater.class);
  private static final int MAX_WAIT_SECONDS = 30;
  private static final int SECONDS_PER_WAIT = 1;

  private final ProgramId programId;
  private final TwillController twillController;
  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final Multimap<String, QueueName> consumerQueues;
  private final TransactionExecutorFactory txExecutorFactory;
  private final Impersonator impersonator;

  DistributedFlowletInstanceUpdater(ProgramId programId, TwillController twillController, QueueAdmin queueAdmin,
                                    StreamAdmin streamAdmin, Multimap<String, QueueName> consumerQueues,
                                    TransactionExecutorFactory txExecutorFactory, Impersonator impersonator) {
    this.programId = programId;
    this.twillController = twillController;
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.consumerQueues = consumerQueues;
    this.txExecutorFactory = txExecutorFactory;
    this.impersonator = impersonator;
  }

  void update(final String flowletId, final int newInstanceCount, FlowSpecification flowSpec) throws Exception {
    // Find all flowlets that are source of the given flowletId.
    Set<String> flowlets = getUpstreamFlowlets(flowSpec, flowletId, Sets.<String>newHashSet());
    flowlets.add(flowletId);

    // Suspend all upstream flowlets and the flowlet that is going to change instances
    for (String id : flowlets) {
      waitForInstances(id, getInstances(flowSpec, id));
      // Need to suspend one by one due to a bug in Twill (TWILL-123)
      twillController.sendCommand(id, ProgramCommands.SUSPEND).get();
    }

    impersonator.doAs(programId.getNamespaceId(), new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        FlowUtils.reconfigure(consumerQueues.get(flowletId),
                              FlowUtils.generateConsumerGroupId(programId, flowletId), newInstanceCount,
                              streamAdmin, queueAdmin, txExecutorFactory);
        return null;
      }
    });

    twillController.changeInstances(flowletId, newInstanceCount).get();
    for (String id : flowlets) {
      twillController.sendCommand(id, ProgramCommands.RESUME).get();
    }
  }

  // wait until there are expectedInstances of the flowlet.  This is needed to prevent the case where a suspend
  // command is sent before all flowlet instances have been registered in ZK, and then the change instance command
  // is sent after the new flowlet instances have started up, which will cause them to crash because
  // it cannot change instances without being in the suspended state.
  private void waitForInstances(String flowletId, int expectedInstances) throws InterruptedException, TimeoutException {
    int numRunningFlowlets = getNumberOfProvisionedInstances(flowletId);
    int secondsWaited = 0;
    while (numRunningFlowlets != expectedInstances) {
      LOG.debug("waiting for {} instances of {} before suspending flowlets", expectedInstances, flowletId);
      TimeUnit.SECONDS.sleep(SECONDS_PER_WAIT);
      secondsWaited += SECONDS_PER_WAIT;
      if (secondsWaited > MAX_WAIT_SECONDS) {
        String errmsg =
          String.format("waited %d seconds for instances of %s to reach expected count of %d, but %d are running",
                                      secondsWaited, flowletId, expectedInstances, numRunningFlowlets);
        LOG.error(errmsg);
        throw new TimeoutException(errmsg);
      }
      numRunningFlowlets = getNumberOfProvisionedInstances(flowletId);
    }
  }

  private int getNumberOfProvisionedInstances(String flowletId) {
    return twillController.getResourceReport().getRunnableResources(flowletId).size();
  }

  private <T extends Collection<String>> T getUpstreamFlowlets(FlowSpecification flowSpec, String flowletId, T result) {
    for (FlowletConnection connection : flowSpec.getConnections()) {
      if (connection.getTargetName().equals(flowletId)
        && connection.getSourceType() == FlowletConnection.Type.FLOWLET) {
        result.add(connection.getSourceName());
      }
    }
    return result;
  }

  private int getInstances(FlowSpecification flowSpec, String flowletId) {
    return flowSpec.getFlowlets().get(flowletId).getInstances();
  }
}

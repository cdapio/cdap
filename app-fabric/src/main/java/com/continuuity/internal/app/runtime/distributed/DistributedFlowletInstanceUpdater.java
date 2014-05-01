package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.internal.app.runtime.flow.FlowUtils;
import com.google.common.collect.Multimap;
import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * For updating number of flowlet instances
 */
final class DistributedFlowletInstanceUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedFlowletInstanceUpdater.class);
  private static final int MAX_WAIT_SECONDS = 30;
  private static final int SECONDS_PER_WAIT = 1;

  private final Program program;
  private final TwillController twillController;
  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final Multimap<String, QueueName> consumerQueues;

  DistributedFlowletInstanceUpdater(Program program, TwillController twillController, QueueAdmin queueAdmin,
                                    StreamAdmin streamAdmin, Multimap<String, QueueName> consumerQueues) {
    this.program = program;
    this.twillController = twillController;
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.consumerQueues = consumerQueues;
  }

  void update(String flowletId, int newInstanceCount, int oldInstanceCount) throws Exception {
    waitForInstances(flowletId, oldInstanceCount);
    twillController.sendCommand(flowletId, ProgramCommands.SUSPEND).get();

    FlowUtils.reconfigure(consumerQueues.get(flowletId),
                          FlowUtils.generateConsumerGroupId(program, flowletId), newInstanceCount,
                          streamAdmin, queueAdmin);

    twillController.changeInstances(flowletId, newInstanceCount).get();
    twillController.sendCommand(flowletId, ProgramCommands.RESUME).get();
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
}

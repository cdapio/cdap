package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.continuuity.internal.app.runtime.flow.FlowUtils;
import com.continuuity.weave.api.WeaveController;
import com.google.common.collect.Multimap;

/**
 * For updating number of flowlet instances
 */
final class DistributedFlowletInstanceUpdater {

  private final Program program;
  private final WeaveController weaveController;
  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final Multimap<String, QueueName> consumerQueues;

  DistributedFlowletInstanceUpdater(Program program, WeaveController weaveController, QueueAdmin queueAdmin,
                                    StreamAdmin streamAdmin, Multimap<String, QueueName> consumerQueues) {
    this.program = program;
    this.weaveController = weaveController;
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.consumerQueues = consumerQueues;
  }

  void update(String flowletId, int newInstanceCount) throws Exception {
    weaveController.sendCommand(flowletId, ProgramCommands.SUSPEND).get();

    for (QueueName queueName : consumerQueues.get(flowletId)) {
      if (queueName.isStream()) {
        streamAdmin.configureInstances(queueName, FlowUtils.generateConsumerGroupId(program, flowletId),
                                       newInstanceCount);
      } else {
        queueAdmin.configureInstances(queueName, FlowUtils.generateConsumerGroupId(program, flowletId),
                                      newInstanceCount);
      }
    }

    weaveController.changeInstances(flowletId, newInstanceCount).get();
    weaveController.sendCommand(flowletId, ProgramCommands.RESUME).get();

  }
}

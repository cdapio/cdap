/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.DisableTransaction;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.weave.AbortOnTimeoutEventHandler;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.app.runtime.flow.FlowUtils;
import com.continuuity.weave.api.EventHandler;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class DistributedFlowProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedFlowProgramRunner.class);

  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;

  @Inject
  DistributedFlowProgramRunner(WeaveRunner weaveRunner, Configuration hConfig,
                               CConfiguration cConfig, QueueAdmin queueAdmin, StreamAdmin streamAdmin) {
    super(weaveRunner, hConfig, cConfig);
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options,
                                     File hConfFile, File cConfFile, ApplicationLauncher launcher) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.FLOW, "Only FLOW process type is supported.");

    try {
      boolean disableTransaction = program.getMainClass().isAnnotationPresent(DisableTransaction.class);

      if (disableTransaction) {
        LOG.info("Transaction is disable for flow {}.{}", program.getApplicationId(), program.getId().getId());
      }

      FlowSpecification flowSpec = appSpec.getFlows().get(program.getName());
      Preconditions.checkNotNull(flowSpec, "Missing FlowSpecification for %s", program.getName());

      LOG.info("Configuring flowlets queues");
      Multimap<String, QueueName> flowletQueues = configureQueue(program, flowSpec);

      // Launch flowlet program runners
      LOG.info("Launching distributed flow: " + program.getName() + ":" + flowSpec.getName());

      WeaveController controller = launcher.launch(new FlowWeaveApplication(program, flowSpec,
                                                                            hConfFile, cConfFile,
                                                                            disableTransaction, eventHandler));
      DistributedFlowletInstanceUpdater instanceUpdater = new DistributedFlowletInstanceUpdater(program, controller,
                                                                                                queueAdmin, streamAdmin,
                                                                                                flowletQueues);
      return new FlowWeaveProgramController(program.getName(), controller, instanceUpdater).startListen();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected EventHandler createEventHandler(CConfiguration cConf) {
    return new AbortOnTimeoutEventHandler(
      cConf.getLong(Constants.CFG_WEAVE_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE), true);
  }

  /**
   * Configures all queues being used in this flow.
   *
   * @return A Multimap from flowletId to QueueName where the flowlet is a consumer of.
   */
  private Multimap<String, QueueName> configureQueue(Program program, FlowSpecification flowSpec) {
    // Generate all queues specifications
    Id.Application appId = Id.Application.from(program.getAccountId(), program.getApplicationId());
    Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> queueSpecs
      = new SimpleQueueSpecificationGenerator(appId).create(flowSpec);

    // For each queue in the flow, gather a map of consumer groupId to number of instances
    Table<QueueName, Long, Integer> queueConfigs = HashBasedTable.create();

    // For storing result from flowletId to queue.
    ImmutableSetMultimap.Builder<String, QueueName> resultBuilder = ImmutableSetMultimap.builder();

    // Loop through each flowlet
    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      String flowletId = entry.getKey();
      long groupId = FlowUtils.generateConsumerGroupId(program, flowletId);
      int instances = entry.getValue().getInstances();

      // For each queue that the flowlet is a consumer, store the number of instances for this flowlet
      for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.column(flowletId).values())) {
        queueConfigs.put(queueSpec.getQueueName(), groupId, instances);
        resultBuilder.put(flowletId, queueSpec.getQueueName());
      }
    }

    try {
      // For each queue in the flow, configure it through QueueAdmin
      for (Map.Entry<QueueName, Map<Long, Integer>> row : queueConfigs.rowMap().entrySet()) {
        LOG.info("Queue config for {} : {}", row.getKey(), row.getValue());
        if (row.getKey().isStream()) {
          streamAdmin.configureGroups(row.getKey(), row.getValue());
        } else {
          queueAdmin.configureGroups(row.getKey(), row.getValue());
        }
      }
      return resultBuilder.build();
    } catch (Exception e) {
      LOG.error("Failed to configure queues", e);
      throw Throwables.propagate(e);
    }
  }
}

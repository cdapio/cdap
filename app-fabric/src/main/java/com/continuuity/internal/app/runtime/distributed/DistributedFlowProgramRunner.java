/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.annotation.DisableTransaction;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.twill.AbortOnTimeoutEventHandler;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.internal.app.runtime.flow.FlowUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 */
public final class DistributedFlowProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedFlowProgramRunner.class);

  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;

  @Inject
  DistributedFlowProgramRunner(TwillRunner twillRunner, Configuration hConfig,
                               CConfiguration cConfig, QueueAdmin queueAdmin, StreamAdmin streamAdmin) {
    super(twillRunner, hConfig, cConfig);
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
      Multimap<String, QueueName> flowletQueues = FlowUtils.configureQueue(program, flowSpec, streamAdmin, queueAdmin);

      // Launch flowlet program runners
      LOG.info("Launching distributed flow: " + program.getName() + ":" + flowSpec.getName());

      TwillController controller = launcher.launch(new FlowTwillApplication(program, flowSpec,
                                                                            hConfFile, cConfFile,
                                                                            disableTransaction, eventHandler));
      DistributedFlowletInstanceUpdater instanceUpdater = new DistributedFlowletInstanceUpdater(program, controller,
                                                                                                queueAdmin, streamAdmin,
                                                                                                flowletQueues);
      return new FlowTwillProgramController(program.getName(), controller, instanceUpdater).startListen();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected EventHandler createEventHandler(CConfiguration cConf) {
    return new AbortOnTimeoutEventHandler(
      cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE), true);
  }
}

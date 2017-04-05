/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.queue.QueueSpecification;
import co.cask.cdap.app.queue.QueueSpecificationGenerator;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.twill.AbortOnTimeoutEventHandler;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.queue.SimpleQueueSpecificationGenerator;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Set;

/**
 * A {@link ProgramRunner} to start a {@link Flow} program in distributed mode.
 */
public final class DistributedFlowProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedFlowProgramRunner.class);

  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final TransactionExecutorFactory txExecutorFactory;
  private final Impersonator impersonator;

  @Inject
  DistributedFlowProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf,
                               CConfiguration cConfig, QueueAdmin queueAdmin, StreamAdmin streamAdmin,
                               TransactionExecutorFactory txExecutorFactory,
                               TokenSecureStoreRenewer tokenSecureStoreRenewer,
                               Impersonator impersonator) {
    super(twillRunner,  hConf, cConfig, tokenSecureStoreRenewer, impersonator);
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.txExecutorFactory = txExecutorFactory;
    this.impersonator = impersonator;
  }

  @Override
  protected void validateOptions(Program program, ProgramOptions options) {
    super.validateOptions(program, options);
    FlowSpecification spec = program.getApplicationSpecification().getFlows().get(program.getName());
    for (String flowlet : spec.getFlowlets().keySet()) {
      SystemArguments.validateTransactionTimeout(options.getUserArguments().asMap(), cConf, "flowlet", flowlet);
    }
  }

  @Override
  public ProgramController createProgramController(TwillController twillController,
                                                   ProgramDescriptor programDescriptor, RunId runId) {
    FlowSpecification flowSpec = programDescriptor.getSpecification();
    DistributedFlowletInstanceUpdater instanceUpdater = new DistributedFlowletInstanceUpdater(
      programDescriptor.getProgramId(), twillController, queueAdmin, streamAdmin,
      getFlowletQueues(programDescriptor.getProgramId().getParent(), flowSpec),
      txExecutorFactory, impersonator
    );
    return createProgramController(twillController, programDescriptor.getProgramId(), runId, instanceUpdater);
  }

  private ProgramController createProgramController(TwillController twillController, ProgramId programId, RunId runId,
                                                    DistributedFlowletInstanceUpdater instanceUpdater) {
    return new FlowTwillProgramController(programId, twillController, instanceUpdater, runId).startListen();
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options,
                                     Map<String, LocalizeResource> localizeResources,
                                     File tempDir, ApplicationLauncher launcher) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.FLOW, "Only FLOW process type is supported.");

    try {
      FlowSpecification flowSpec = appSpec.getFlows().get(program.getName());
      Preconditions.checkNotNull(flowSpec, "Missing FlowSpecification for %s", program.getName());

      LOG.info("Configuring flowlets queues");
      Multimap<String, QueueName> flowletQueues = FlowUtils.configureQueue(program, flowSpec,
                                                                           streamAdmin, queueAdmin, txExecutorFactory);

      // Launch flowlet program runners
      RunId runId = ProgramRunners.getRunId(options);
      LOG.info("Launching distributed flow: {}", program.getId().run(runId));

      TwillController controller = launcher.launch(new FlowTwillApplication(program, options.getUserArguments(),
                                                                            flowSpec, localizeResources, eventHandler));
      DistributedFlowletInstanceUpdater instanceUpdater =
        new DistributedFlowletInstanceUpdater(program.getId(), controller, queueAdmin,
                                              streamAdmin, flowletQueues, txExecutorFactory, impersonator);

      return createProgramController(controller, program.getId(), runId, instanceUpdater);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected EventHandler createEventHandler(CConfiguration cConf) {
    return new AbortOnTimeoutEventHandler(
      cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE), true);
  }

  @Override
  protected TwillPreparer setLogLevels(TwillPreparer twillPreparer, Program program, ProgramOptions options) {
    FlowSpecification spec = program.getApplicationSpecification().getFlows().get(program.getName());
    for (String flowlet : spec.getFlowlets().keySet()) {
      Map<String, String> logLevels = SystemArguments.getLogLevels(
        RuntimeArguments.extractScope(FlowUtils.FLOWLET_SCOPE, flowlet, options.getUserArguments().asMap()));
      if (!logLevels.isEmpty()) {
        twillPreparer.setLogLevels(flowlet, transformLogLevels(logLevels));
      }
    }
    return twillPreparer;
  }

  /**
   * Gets the queue configuration of the Flow based on the connections in the given {@link FlowSpecification}.
   */
  private Multimap<String, QueueName> getFlowletQueues(ApplicationId appId, FlowSpecification flowSpec) {
    // Generate all queues specifications
    Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> queueSpecs
      = new SimpleQueueSpecificationGenerator(appId).create(flowSpec);

    // For storing result from flowletId to queue.
    ImmutableSetMultimap.Builder<String, QueueName> resultBuilder = ImmutableSetMultimap.builder();

    // Loop through each flowlet
    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      String flowletId = entry.getKey();
      // For each queue that the flowlet is a consumer, store the number of instances for this flowlet
      for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.column(flowletId).values())) {
        resultBuilder.put(flowletId, queueSpec.getQueueName());
      }
    }
    return resultBuilder.build();
  }
}

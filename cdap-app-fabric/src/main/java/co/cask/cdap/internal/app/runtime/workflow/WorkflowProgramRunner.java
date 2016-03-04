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
package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;

import java.net.InetAddress;

/**
 * A {@link ProgramRunner} that runs a {@link Workflow}.
 */
public class WorkflowProgramRunner implements ProgramRunner {
  private final ServiceAnnouncer serviceAnnouncer;
  private final InetAddress hostname;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final Store store;
  private final StreamAdmin streamAdmin;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final UsageRegistry usageRegistry;

  @Inject
  public WorkflowProgramRunner(ServiceAnnouncer serviceAnnouncer,
                               @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                               MetricsCollectionService metricsCollectionService, DatasetFramework datasetFramework,
                               DiscoveryServiceClient discoveryServiceClient, TransactionSystemClient txClient,
                               Store store, CConfiguration cConf, Configuration hConf, StreamAdmin streamAdmin,
                               LocationFactory locationFactory, UsageRegistry usageRegistry) {
    this.serviceAnnouncer = serviceAnnouncer;
    this.hostname = hostname;
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.store = store;
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.streamAdmin = streamAdmin;
    this.usageRegistry = usageRegistry;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify options
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(program.getName());
    Preconditions.checkNotNull(workflowSpec, "Missing WorkflowSpecification for %s", program.getName());

    RunId runId = RunIds.fromString(options.getArguments().getOption(ProgramOptionConstants.RUN_ID));

    // Setup dataset framework context, if required
    if (datasetFramework instanceof ProgramContextAware) {
      Id.Program programId = program.getId();
      ((ProgramContextAware) datasetFramework).initContext(new Id.Run(programId, runId.getId()));
    }

    WorkflowDriver driver = new WorkflowDriver(program, options, hostname, workflowSpec, metricsCollectionService,
                                               datasetFramework, discoveryServiceClient, txClient, store, cConf, hConf,
                                               locationFactory, streamAdmin, usageRegistry);

    // Controller needs to be created before starting the driver so that the state change of the driver
    // service can be fully captured by the controller.
    ProgramController controller = new WorkflowProgramController(program, driver, serviceAnnouncer, runId);
    driver.start();

    return controller;
  }
}

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
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.internal.app.runtime.AbstractProgramRunnerWithPlugin;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.net.InetAddress;

/**
 * A {@link ProgramRunner} that runs a {@link Workflow}.
 */
public class WorkflowProgramRunner extends AbstractProgramRunnerWithPlugin {

  private final ProgramRunnerFactory programRunnerFactory;
  private final ServiceAnnouncer serviceAnnouncer;
  private final InetAddress hostname;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final RuntimeStore runtimeStore;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final CConfiguration cConf;

  @Inject
  public WorkflowProgramRunner(ProgramRunnerFactory programRunnerFactory, ServiceAnnouncer serviceAnnouncer,
                               @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname,
                               MetricsCollectionService metricsCollectionService, DatasetFramework datasetFramework,
                               DiscoveryServiceClient discoveryServiceClient, TransactionSystemClient txClient,
                               RuntimeStore runtimeStore, CConfiguration cConf, SecureStore secureStore,
                               SecureStoreManager secureStoreManager) {
    super(cConf);
    this.programRunnerFactory = programRunnerFactory;
    this.serviceAnnouncer = serviceAnnouncer;
    this.hostname = hostname;
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.runtimeStore = runtimeStore;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.cConf = cConf;
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

    RunId runId = ProgramRunners.getRunId(options);

    // Setup dataset framework context, if required
    if (datasetFramework instanceof ProgramContextAware) {
      ProgramId programId = program.getId();
      ((ProgramContextAware) datasetFramework).initContext(programId.run(runId));
    }


    PluginInstantiator pluginInstantiator = createPluginInstantiator(options, program.getClassLoader());
    WorkflowDriver driver = new WorkflowDriver(program, options, hostname, workflowSpec, programRunnerFactory,
                                               metricsCollectionService, datasetFramework, discoveryServiceClient,
                                               txClient, runtimeStore, cConf, pluginInstantiator,
                                               secureStore, secureStoreManager);
    // Controller needs to be created before starting the driver so that the state change of the driver
    // service can be fully captured by the controller.
    ProgramController controller = new WorkflowProgramController(program, driver, serviceAnnouncer, runId);
    driver.start();

    return controller;
  }
}

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
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.ProgramContextAware;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractProgramRunnerWithPlugin;
import co.cask.cdap.internal.app.runtime.BasicProgramContext;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ProgramRunner} that runs a {@link Workflow}.
 */
public class WorkflowProgramRunner extends AbstractProgramRunnerWithPlugin {

  private final ProgramRunnerFactory programRunnerFactory;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final WorkflowStateWriter workflowStateWriter;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final MessagingService messagingService;
  private final CConfiguration cConf;
  private final ProgramStateWriter programStateWriter;

  @Inject
  public WorkflowProgramRunner(ProgramRunnerFactory programRunnerFactory,
                               MetricsCollectionService metricsCollectionService, DatasetFramework datasetFramework,
                               DiscoveryServiceClient discoveryServiceClient, TransactionSystemClient txClient,
                               WorkflowStateWriter workflowStateWriter, CConfiguration cConf, SecureStore secureStore,
                               SecureStoreManager secureStoreManager, MessagingService messagingService,
                               ProgramStateWriter programStateWriter) {
    super(cConf);
    this.programRunnerFactory = programRunnerFactory;
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.workflowStateWriter = workflowStateWriter;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.messagingService = messagingService;
    this.cConf = cConf;
    this.programStateWriter = programStateWriter;
  }

  @Override
  public ProgramController run(final Program program, final ProgramOptions options) {
    // Extract and verify options
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(program.getName());
    Preconditions.checkNotNull(workflowSpec, "Missing WorkflowSpecification for %s", program.getName());

    final RunId runId = ProgramRunners.getRunId(options);

    // Setup dataset framework context, if required
    if (datasetFramework instanceof ProgramContextAware) {
      ProgramId programId = program.getId();
      ((ProgramContextAware) datasetFramework).setContext(new BasicProgramContext(programId.run(runId)));
    }

    // List of all Closeable resources that needs to be cleanup
    final List<Closeable> closeables = new ArrayList<>();
    try {
      PluginInstantiator pluginInstantiator = createPluginInstantiator(options, program.getClassLoader());
      if (pluginInstantiator != null) {
        closeables.add(pluginInstantiator);
      }

      WorkflowDriver driver = new WorkflowDriver(program, options, workflowSpec, programRunnerFactory,
                                                 metricsCollectionService, datasetFramework, discoveryServiceClient,
                                                 txClient, workflowStateWriter, cConf, pluginInstantiator,
                                                 secureStore, secureStoreManager, messagingService, programStateWriter);

      // Controller needs to be created before starting the driver so that the state change of the driver
      // service can be fully captured by the controller.
      ProgramController controller = new WorkflowProgramController(program.getId().run(runId), driver);
      driver.start();
      return controller;
    } catch (Exception e) {
      closeAllQuietly(closeables);
      throw Throwables.propagate(e);
    }
  }
}

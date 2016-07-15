/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.worker;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.stream.StreamWriterFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.internal.app.runtime.AbstractProgramRunnerWithPlugin;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;

/**
 * A {@link ProgramRunner} that runs a {@link Worker}.
 */
public class WorkerProgramRunner extends AbstractProgramRunnerWithPlugin {
  private static final Gson GSON = new Gson();

  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final StreamWriterFactory streamWriterFactory;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;

  @Inject
  public WorkerProgramRunner(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                             DatasetFramework datasetFramework, DiscoveryServiceClient discoveryServiceClient,
                             TransactionSystemClient txClient, StreamWriterFactory streamWriterFactory,
                             SecureStore secureStore, SecureStoreManager secureStoreManager) {
    super(cConf);
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.streamWriterFactory = streamWriterFactory;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
    Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

    int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
    Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

    RunId runId = ProgramRunners.getRunId(options);

    ProgramType programType = program.getType();
    Preconditions.checkNotNull(programType, "Missing processor type.");
    Preconditions.checkArgument(programType == ProgramType.WORKER, "Only Worker process type is supported.");

    WorkerSpecification workerSpec = appSpec.getWorkers().get(program.getName());
    Preconditions.checkArgument(workerSpec != null, "Missing Worker specification for {}", program.getId());
    String instances = options.getArguments().getOption(ProgramOptionConstants.INSTANCES,
                                                        String.valueOf(workerSpec.getInstances()));
    String resourceString = options.getArguments().getOption(ProgramOptionConstants.RESOURCES, null);
    Resources newResources = (resourceString != null) ? GSON.fromJson(resourceString, Resources.class) :
      workerSpec.getResources();

    WorkerSpecification newWorkerSpec = new WorkerSpecification(workerSpec.getClassName(), workerSpec.getName(),
                                                                workerSpec.getDescription(), workerSpec.getProperties(),
                                                                workerSpec.getDatasets(), newResources,
                                                                Integer.valueOf(instances));

    // Setup dataset framework context, if required
    if (datasetFramework instanceof ProgramContextAware) {
      Id.Program programId = program.getId();
      ((ProgramContextAware) datasetFramework).initContext(new Id.Run(programId, runId.getId()));
    }

    final PluginInstantiator pluginInstantiator = createPluginInstantiator(options, program.getClassLoader());
    try {
      BasicWorkerContext context = new BasicWorkerContext(newWorkerSpec, program, options, instanceId, instanceCount,
                                                          metricsCollectionService, datasetFramework, txClient,
                                                          discoveryServiceClient, streamWriterFactory,
                                                          pluginInstantiator, secureStore, secureStoreManager);

      WorkerDriver worker = new WorkerDriver(program, newWorkerSpec, context);

      // Add a service listener to make sure the plugin instantiator is closed when the worker driver finished.
      worker.addListener(new ServiceListenerAdapter() {
        @Override
        public void terminated(Service.State from) {
          Closeables.closeQuietly(pluginInstantiator);
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
          Closeables.closeQuietly(pluginInstantiator);
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      ProgramController controller = new WorkerControllerServiceAdapter(worker, program.getId(), runId,
                                                                        workerSpec.getName() + "-" + instanceId);
      worker.start();
      return controller;
    } catch (Throwable t) {
      Closeables.closeQuietly(pluginInstantiator);
      throw t;
    }
  }

  private static final class WorkerControllerServiceAdapter extends ProgramControllerServiceAdapter {
    private final WorkerDriver workerDriver;

    WorkerControllerServiceAdapter(WorkerDriver workerDriver, Id.Program programId, RunId runId, String componentName) {
      super(workerDriver, programId, runId, componentName);
      this.workerDriver = workerDriver;
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      super.doCommand(name, value);
      if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Integer)) {
        return;
      }

      workerDriver.setInstanceCount((Integer) value);
    }
  }
}

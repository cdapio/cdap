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
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.app.stream.StreamWriterFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.internal.app.runtime.AbstractProgramRunner;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.Closeable;
import java.util.Deque;
import javax.annotation.Nullable;

/**
 * A {@link ProgramRunner} that runs a {@link Worker}.
 */
public class WorkerProgramRunner extends AbstractProgramRunner {
  private static final Gson GSON = new Gson();

  private final MetricsCollectionService metricsCollectionService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final StreamWriterFactory streamWriterFactory;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;

  @Inject
  public WorkerProgramRunner(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                             DatasetFramework datasetFramework, DiscoveryServiceClient discoveryServiceClient,
                             TransactionSystemClient txClient, StreamWriterFactory streamWriterFactory,
                             SecureStore secureStore, SecureStoreManager secureStoreManager,
                             RuntimeStore runtimeStore) {
    super(cConf, runtimeStore, datasetFramework);
    this.metricsCollectionService = metricsCollectionService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.streamWriterFactory = streamWriterFactory;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  @Override
  protected ProgramController startProgram(Program program, ProgramOptions options, RunId runId,
                                           DatasetFramework datasetFramework,
                                           final @Nullable PluginInstantiator pluginInstantiator,
                                           @Nullable WorkflowProgramInfo workflowInfo,
                                           Deque<Closeable> closeables) {

    int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
    Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

    int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
    Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

    ProgramType programType = program.getType();
    Preconditions.checkNotNull(programType, "Missing processor type.");
    Preconditions.checkArgument(programType == ProgramType.WORKER, "Only Worker process type is supported.");

    WorkerSpecification workerSpec = program.getApplicationSpecification().getWorkers().get(program.getName());
    Preconditions.checkArgument(workerSpec != null, "Missing Worker specification for %s", program.getId());
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
      ProgramId programId = program.getId();
      ((ProgramContextAware) datasetFramework).initContext(programId.run(runId));
    }

    BasicWorkerContext context = new BasicWorkerContext(newWorkerSpec, program, options, instanceId, instanceCount,
                                                        metricsCollectionService, datasetFramework, txClient,
                                                        discoveryServiceClient, streamWriterFactory,
                                                        pluginInstantiator, secureStore, secureStoreManager);

    WorkerDriver worker = new WorkerDriver(program, newWorkerSpec, context);

    ProgramController controller = new WorkerControllerServiceAdapter(worker, program.getId(), runId,
                                                                      workerSpec.getName() + "-" + instanceId);
    worker.start();
    return controller;
  }

  private static final class WorkerControllerServiceAdapter extends ProgramControllerServiceAdapter {
    private final WorkerDriver workerDriver;

    WorkerControllerServiceAdapter(WorkerDriver workerDriver, ProgramId programId, RunId runId, String componentName) {
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

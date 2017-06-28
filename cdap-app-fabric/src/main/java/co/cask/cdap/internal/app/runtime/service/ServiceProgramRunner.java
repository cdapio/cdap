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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.ProgramContextAware;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractProgramRunnerWithPlugin;
import co.cask.cdap.internal.app.runtime.BasicProgramContext;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.artifact.DefaultArtifactManager;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.services.ServiceHttpServer;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.ServiceListenerAdapter;

/**
 * A {@link ProgramRunner} that runs an HTTP Server inside a Service.
 */
public class ServiceProgramRunner extends AbstractProgramRunnerWithPlugin {

  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final ServiceAnnouncer serviceAnnouncer;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final MessagingService messagingService;
  private final DefaultArtifactManager defaultArtifactManager;

  @Inject
  public ServiceProgramRunner(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                              DatasetFramework datasetFramework, DiscoveryServiceClient discoveryServiceClient,
                              TransactionSystemClient txClient, ServiceAnnouncer serviceAnnouncer,
                              SecureStore secureStore, SecureStoreManager secureStoreManager,
                              MessagingService messagingService,
                              DefaultArtifactManager defaultArtifactManager) {
    super(cConf);
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.serviceAnnouncer = serviceAnnouncer;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.messagingService = messagingService;
    this.defaultArtifactManager = defaultArtifactManager;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
    Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

    int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
    Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

    RunId runId = ProgramRunners.getRunId(options);

    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType programType = program.getType();
    Preconditions.checkNotNull(programType, "Missing processor type.");
    Preconditions.checkArgument(programType == ProgramType.SERVICE, "Only Service process type is supported.");

    ServiceSpecification spec = appSpec.getServices().get(program.getName());

    String host = options.getArguments().getOption(ProgramOptionConstants.HOST);
    Preconditions.checkArgument(host != null, "No hostname is provided");

    // Setup dataset framework context, if required
    if (datasetFramework instanceof ProgramContextAware) {
      ProgramId programId = program.getId();
      ((ProgramContextAware) datasetFramework).setContext(new BasicProgramContext(programId.run(runId)));
    }

    final PluginInstantiator pluginInstantiator = createPluginInstantiator(options, program.getClassLoader());
    try {
      ServiceHttpServer component = new ServiceHttpServer(host, program, options, cConf, spec,
                                                          instanceId, instanceCount, serviceAnnouncer,
                                                          metricsCollectionService, datasetFramework,
                                                          txClient, discoveryServiceClient,
                                                          pluginInstantiator, secureStore, secureStoreManager,
                                                          messagingService, defaultArtifactManager);

      // Add a service listener to make sure the plugin instantiator is closed when the http server is finished.
      component.addListener(new ServiceListenerAdapter() {
        @Override
        public void terminated(Service.State from) {
          Closeables.closeQuietly(pluginInstantiator);
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
          Closeables.closeQuietly(pluginInstantiator);
        }
      }, Threads.SAME_THREAD_EXECUTOR);


      ProgramController controller = new ServiceProgramControllerAdapter(component, program.getId(), runId,
                                                                         spec.getName() + "-" + instanceId);
      component.start();
      return controller;
    } catch (Throwable t) {
      Closeables.closeQuietly(pluginInstantiator);
      throw t;
    }
  }

  private static final class ServiceProgramControllerAdapter extends ProgramControllerServiceAdapter {
    private final ServiceHttpServer service;

    ServiceProgramControllerAdapter(ServiceHttpServer service, ProgramId programId,
                                    RunId runId, String componentName) {
      super(service, programId, runId, componentName);
      this.service = service;
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      super.doCommand(name, value);
      if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Integer)) {
        return;
      }
      service.setInstanceCount((Integer) value);
    }
  }
}

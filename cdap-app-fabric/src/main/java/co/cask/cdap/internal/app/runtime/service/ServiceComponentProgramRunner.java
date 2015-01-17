/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.DataFabricFacadeFactory;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.service.http.BasicHttpServiceContext;
import co.cask.cdap.internal.app.services.BasicHttpServiceContextFactory;
import co.cask.cdap.internal.app.services.ServiceHttpServer;
import co.cask.cdap.internal.app.services.ServiceWorkerDriver;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.RunIds;

/**
 * A {@link ProgramRunner} that runs a component inside a Service (either a HTTP Server or a Worker).
 */
public class ServiceComponentProgramRunner implements ProgramRunner {

  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DataFabricFacadeFactory dataFabricFacadeFactory;

  @Inject
  public ServiceComponentProgramRunner(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                                       DatasetFramework datasetFramework, DiscoveryServiceClient discoveryServiceClient,
                                       TransactionSystemClient txClient, ServiceAnnouncer serviceAnnouncer,
                                       DataFabricFacadeFactory dataFabricFacadeFactory) {
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.serviceAnnouncer = serviceAnnouncer;
    this.dataFabricFacadeFactory = dataFabricFacadeFactory;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    String componentName = options.getName();
    Preconditions.checkNotNull(componentName, "Missing service component name.");

    int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
    Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

    int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
    Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

    String runIdOption = options.getArguments().getOption(ProgramOptionConstants.RUN_ID);
    Preconditions.checkNotNull(runIdOption, "Missing runId");
    RunId runId = RunIds.fromString(runIdOption);

    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType programType = program.getType();
    Preconditions.checkNotNull(programType, "Missing processor type.");
    Preconditions.checkArgument(programType == ProgramType.SERVICE, "Only Service process type is supported.");

    ServiceSpecification spec = appSpec.getServices().get(program.getName());

    // By convention, the Http service always has the same name as the service itself.
    Service component;
    if (componentName.equals(program.getName())) {
      // HTTP service
      String host = options.getArguments().getOption(ProgramOptionConstants.HOST);
      Preconditions.checkArgument(host != null, "No hostname is provided");

      component = new ServiceHttpServer(host, program, spec, runId, serviceAnnouncer,
                                        createHttpServiceContextFactory(program, runId, instanceId,
                                        options.getUserArguments()), metricsCollectionService, dataFabricFacadeFactory);
    } else {
      ServiceWorkerSpecification workerSpec = spec.getWorkers().get(componentName);
      Preconditions.checkArgument(workerSpec != null, "Missing service worker specification for {}", program.getId());

      BasicServiceWorkerContext context = new BasicServiceWorkerContext(workerSpec, program, runId,
                                                                        instanceId, instanceCount,
                                                                        options.getUserArguments(), cConf,
                                                                        metricsCollectionService, datasetFramework,
                                                                        txClient, discoveryServiceClient);
      component = new ServiceWorkerDriver(program, workerSpec, context);
    }

    ProgramControllerServiceAdapter controller = new ProgramControllerServiceAdapter(component, componentName, runId);
    component.start();
    return controller;
  }

  private BasicHttpServiceContextFactory createHttpServiceContextFactory(final Program program,
                                                                         final RunId runId,
                                                                         final int instanceId,
                                                                         final Arguments runtimeArgs) {
    return new BasicHttpServiceContextFactory() {
      @Override
      public BasicHttpServiceContext create(HttpServiceHandlerSpecification spec) {
        return new BasicHttpServiceContext(spec, program, runId, instanceId, runtimeArgs,
                                           metricsCollectionService, datasetFramework, cConf,
                                           discoveryServiceClient, txClient);
      }
    };
  }
}

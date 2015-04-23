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

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.DataFabricFacadeFactory;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.ServiceHttpServer;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * A {@link ProgramRunner} that runs a component inside a Service (either a HTTP Server or a Worker).
 */
public class ServiceProgramRunner implements ProgramRunner {

  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final DatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DataFabricFacadeFactory dataFabricFacadeFactory;

  @Inject
  public ServiceProgramRunner(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
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

    String host = options.getArguments().getOption(ProgramOptionConstants.HOST);
    Preconditions.checkArgument(host != null, "No hostname is provided");

    ServiceHttpServer component = new ServiceHttpServer(host, program, spec, runId, options.getUserArguments(),
                                      instanceId, instanceCount, serviceAnnouncer,
                                      metricsCollectionService, datasetFramework, dataFabricFacadeFactory,
                                      txClient, discoveryServiceClient);

    ProgramControllerServiceAdapter controller = new ServiceProgramControllerAdapter(component, componentName, runId);
    component.start();
    return controller;
  }

  private static final class ServiceProgramControllerAdapter extends ProgramControllerServiceAdapter {
    private final ServiceHttpServer service;

    public ServiceProgramControllerAdapter(ServiceHttpServer service, String programName, RunId runId) {
      super(service, programName, runId);
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

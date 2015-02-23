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

import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.AbstractInMemoryProgramRunner;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * For running {@link Service}. Only used in in-memory/standalone mode.
 */
public class ServiceProgramRunner extends AbstractInMemoryProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceProgramRunner.class);

  private final String host;

  @Inject
  ServiceProgramRunner(CConfiguration cConf, ProgramRunnerFactory programRunnerFactory) {
    super(programRunnerFactory);
    this.host = cConf.get(Constants.AppFabric.SERVER_ADDRESS);
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.SERVICE, "Only SERVICE process type is supported.");

    ServiceSpecification serviceSpec = appSpec.getServices().get(program.getName());
    Preconditions.checkNotNull(serviceSpec, "Missing ServiceSpecification for %s", program.getName());

    //RunId for the service
    RunId runId = RunIds.generate();
    Table<String, Integer, ProgramController> components = startAllComponents(program, runId,
                                                                              options.getUserArguments(), serviceSpec);
    return new InMemoryProgramController(components, runId, program, serviceSpec, options.getUserArguments(),
                                         ProgramRunnerFactory.Type.SERVICE_COMPONENT);
  }

  /**
   * Creates and starts all components of the given Service program.
   *
   * @return A {@link Table} with component name in the row, instance id in the column and the {@link ProgramController}
   *         of the component runner as the cell value.
   */
  // TODO: Move this method to AbstractInMemoryProgramRunner to reduce duplication of code.
  private Table<String, Integer, ProgramController> startAllComponents(Program program, RunId runId,
                                                                       Arguments userArguments,
                                                                       ServiceSpecification spec) {
    Table<String, Integer, ProgramController> components = HashBasedTable.create();

    try {
      // Starts the http service. The name is the same as the service name.
      startComponent(program, program.getName(), spec.getInstances(), runId, userArguments, components,
                     ProgramRunnerFactory.Type.SERVICE_COMPONENT);

      // Starts all Workers
      for (Map.Entry<String, ServiceWorkerSpecification> entry : spec.getWorkers().entrySet()) {
        ServiceWorkerSpecification workerSpec = entry.getValue();
        startComponent(program, workerSpec.getName(), workerSpec.getInstances(), runId, userArguments, components,
                       ProgramRunnerFactory.Type.SERVICE_COMPONENT);
      }
    } catch (Throwable t) {
      LOG.error("Failed to start all service components upon startup failure.", t);
      try {
        // Need to stop all started components
        Futures.successfulAsList(Iterables.transform(components.values(),
         new Function<ProgramController, ListenableFuture<?>>() {
           @Override
           public ListenableFuture<?> apply(ProgramController controller) {
             return controller.stop();
           }
         })).get();
        throw Throwables.propagate(t);
      } catch (Exception e) {
        LOG.error("Failed to stop all service components upon startup failure.", e);
        throw Throwables.propagate(e);
      }
    }
    return components;
  }


  @Override
  public ProgramOptions createComponentOptions(String name, int instanceId, int instances, RunId runId,
                                               Arguments userArguments) {
    Map<String, String> options = ImmutableMap.of(ProgramOptionConstants.INSTANCE_ID, Integer.toString(instanceId),
                                                  ProgramOptionConstants.INSTANCES, Integer.toString(instances),
                                                  ProgramOptionConstants.RUN_ID, runId.getId(),
                                                  ProgramOptionConstants.HOST, host);
    return new SimpleProgramOptions(name, new BasicArguments(options), userArguments);
  }
}

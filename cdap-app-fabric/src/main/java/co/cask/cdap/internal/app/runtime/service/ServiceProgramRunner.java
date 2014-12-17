/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.AbstractProgramController;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * For running {@link Service}. Only used in in-memory/standalone mode.
 */
public class ServiceProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceProgramRunner.class);

  private final String host;
  private final ProgramRunnerFactory programRunnerFactory;

  @Inject
  ServiceProgramRunner(CConfiguration cConf, ProgramRunnerFactory programRunnerFactory) {
    this.programRunnerFactory = programRunnerFactory;
    this.host = cConf.get(Constants.AppFabric.SERVER_ADDRESS);
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
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
    return new ServiceProgramController(components, runId, program, serviceSpec, options.getUserArguments());
  }

  /**
   * Creates and starts all components of the given Service program.
   *
   * @return A {@link Table} with component name in the row, instance id in the column and the {@link ProgramController}
   *         of the component runner as the cell value.
   */
  private Table<String, Integer, ProgramController> startAllComponents(Program program, RunId runId,
                                                                       Arguments userArguments,
                                                                       ServiceSpecification spec) {
    Table<String, Integer, ProgramController> components = HashBasedTable.create();

    try {
      // Starts the http service. The name is the same as the service name.
      startComponent(program, program.getName(), spec.getInstances(), runId, userArguments, components);

      // Starts all Workers
      for (Map.Entry<String, ServiceWorkerSpecification> entry : spec.getWorkers().entrySet()) {
        ServiceWorkerSpecification workerSpec = entry.getValue();
        startComponent(program, workerSpec.getName(), workerSpec.getInstances(), runId, userArguments, components);
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

  /**
   * Starts all instances of a Service component.
   *
   * @param program The program to run
   * @param name Name of the service component
   * @param instances Number of instances to start
   * @param runId The runId
   * @param userArguments User runtime arguments
   * @param components A Table for storing the resulting ProgramControllers
   */
  private void startComponent(Program program, String name, int instances, RunId runId, Arguments userArguments,
                              Table<String, Integer, ProgramController> components) {
    for (int instanceId = 0; instanceId < instances; instanceId++) {
      ProgramOptions componentOptions = createComponentOptions(name, instanceId, instances, runId, userArguments);
      ProgramController controller = programRunnerFactory.create(ProgramRunnerFactory.Type.SERVICE_COMPONENT)
        .run(program, componentOptions);
      components.put(name, instanceId, controller);
    }
  }

  private ProgramOptions createComponentOptions(String name, int instanceId,
                                                int instances, RunId runId, Arguments userArguments) {
    Map<String, String> options = ImmutableMap.of(ProgramOptionConstants.INSTANCE_ID, Integer.toString(instanceId),
                                                  ProgramOptionConstants.INSTANCES, Integer.toString(instances),
                                                  ProgramOptionConstants.RUN_ID, runId.getId(),
                                                  ProgramOptionConstants.HOST, host);
    return new SimpleProgramOptions(name, new BasicArguments(options), userArguments);
  }

  private final class ServiceProgramController extends AbstractProgramController {
    private final Table<String, Integer, ProgramController> components;
    private final Program program;
    private final ServiceSpecification serviceSpec;
    private final Arguments userArguments;
    private final Lock lock = new ReentrantLock();

    ServiceProgramController(Table<String, Integer, ProgramController> components,
                             RunId runId, Program program, ServiceSpecification serviceSpec,
                             Arguments userArguments) {
      super(program.getName(), runId);
      this.program = program;
      this.components = components;
      this.serviceSpec = serviceSpec;
      this.userArguments = userArguments;
      started();
    }

    @Override
    protected void doSuspend() throws Exception {
      // No-op
    }

    @Override
    protected void doResume() throws Exception {
      // No-op
    }

    @Override
    protected void doStop() throws Exception {
      LOG.info("Stopping Service : " + serviceSpec.getName());
      lock.lock();
      try {
        Futures.successfulAsList(
          Iterables.transform(components.values(),
                              new Function<ProgramController, ListenableFuture<ProgramController>>() {
                                @Override
                                public ListenableFuture<ProgramController> apply(ProgramController input) {
                                  return input.stop();
                                 }
                              }
           )).get();
      } finally {
        lock.unlock();
      }
      LOG.info("Service stopped: " + serviceSpec.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doCommand(String name, Object value) throws Exception {
      if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Map)) {
        return;
      }
      Map<String, String> command = (Map<String, String>) value;
      lock.lock();
      try {
        changeInstances(command.get("runnable"), Integer.valueOf(command.get("newInstances")));
      } catch (Throwable t) {
        LOG.error(String.format("Fail to change instances: %s", command), t);
      } finally {
        lock.unlock();
      }
    }

    /**
     * Change the number of instances of the running runnable.
     * @param runnableName Name of the runnable
     * @param newCount New instance count
     * @throws java.util.concurrent.ExecutionException
     * @throws InterruptedException
     */
    private void changeInstances(String runnableName, final int newCount) throws Exception {
      Map<Integer, ProgramController> liveRunnables = components.row(runnableName);
      int liveCount = liveRunnables.size();
      if (liveCount == newCount) {
        return;
      }

      // stop any extra runnables
      if (liveCount > newCount) {
        List<ListenableFuture<ProgramController>> futures = Lists.newArrayListWithCapacity(liveCount - newCount);
        for (int instanceId = liveCount - 1; instanceId >= newCount; instanceId--) {
          futures.add(components.remove(runnableName, instanceId).stop());
        }
        Futures.allAsList(futures).get();
      }

      // create more runnable instances, if necessary.
      for (int instanceId = liveCount; instanceId < newCount; instanceId++) {
        ProgramOptions options = createComponentOptions(runnableName, instanceId, newCount, getRunId(), userArguments);
        ProgramController controller = programRunnerFactory.create(ProgramRunnerFactory.Type.SERVICE_COMPONENT)
                                                           .run(program, options);
        components.put(runnableName, instanceId, controller);
      }
    }
  }
}

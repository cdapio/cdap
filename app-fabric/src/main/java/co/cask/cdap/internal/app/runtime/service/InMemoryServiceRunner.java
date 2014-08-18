/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * For Running Services in Singlenode.
 */
public class InMemoryServiceRunner implements ProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryServiceRunner.class);
  private final Map<RunId, ProgramOptions> programOptions = Maps.newHashMap();
  private final ProgramRunnerFactory programRunnerFactory;

  @Inject
  InMemoryServiceRunner(ProgramRunnerFactory programRunnerFactory) {
    this.programRunnerFactory = programRunnerFactory;
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

    //RuIid for the service
    RunId runId = RunIds.generate();
    programOptions.put(runId, options);
    final Table<String, Integer, ProgramController> serviceRunnables = createRunnables(program, runId, serviceSpec);
    return new ServiceProgramController(serviceRunnables, runId, program, serviceSpec);
  }

  private Table<String, Integer, ProgramController> createRunnables(Program program, RunId runId,
                                                                    ServiceSpecification serviceSpec) {
    Table<String, Integer, ProgramController> runnables = HashBasedTable.create();

    try {
      for (Map.Entry<String, RuntimeSpecification> entry : serviceSpec.getRunnables().entrySet()) {
        int instanceCount = entry.getValue().getResourceSpecification().getInstances();
        for (int instanceId = 0; instanceId < instanceCount; instanceId++) {
          runnables.put(entry.getKey(), instanceId, startRunnable
            (program, createRunnableOptions(entry.getKey(), instanceId, instanceCount, runId)));
        }
      }
    } catch (Throwable t) {
      // Need to stop all started runnable here.
      try {
        Futures.successfulAsList
          (Iterables.transform(runnables.values(), new Function<ProgramController,
                                 ListenableFuture<ProgramController>>() {
                                 @Override
                                 public ListenableFuture<ProgramController> apply(ProgramController input) {
                                   return input.stop();
                                 }
                               }
           )
          ).get();
      } catch (Exception e) {
        LOG.error("Failed to stop all the runnables");
      }
      throw Throwables.propagate(t);
    }
    return runnables;
  }

  private ProgramController startRunnable(Program program, ProgramOptions runnableOptions) {
    return programRunnerFactory.create(ProgramRunnerFactory.Type.RUNNABLE).run(program, runnableOptions);
  }

  private ProgramOptions createRunnableOptions(String name, int instanceId, int instances, RunId runId) {

    // Get the right user arguments.
    Arguments userArguments = new BasicArguments();
    if (programOptions.containsKey(runId)) {
      userArguments = programOptions.get(runId).getUserArguments();
    }

    return new SimpleProgramOptions(name, new BasicArguments
      (ImmutableMap.of(ProgramOptionConstants.INSTANCE_ID, Integer.toString(instanceId),
                       ProgramOptionConstants.INSTANCES, Integer.toString(instances),
                       ProgramOptionConstants.RUN_ID, runId.getId())), userArguments);
  }

  class ServiceProgramController extends AbstractProgramController {
    private final Table<String, Integer, ProgramController> runnables;
    private final ServiceSpecification serviceSpec;

    ServiceProgramController(Table<String, Integer, ProgramController> runnables,
                             RunId runId, Program program, ServiceSpecification serviceSpec) {
      super(program.getName(), runId);
      this.runnables = runnables;
      this.serviceSpec = serviceSpec;
      started();
    }

    public List<ProgramController> getProgramControllers() {
      return ImmutableList.copyOf(runnables.values());
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
      Futures.successfulAsList
        (Iterables.transform(runnables.values(), new Function<ProgramController,
                               ListenableFuture<ProgramController>>() {
                               @Override
                               public ListenableFuture<ProgramController> apply(ProgramController input) {
                                 return input.stop();
                               }
                             }
         )
        ).get();
      LOG.info("Service stopped: " + serviceSpec.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doCommand(String name, Object value) throws Exception {

    }
  }
}

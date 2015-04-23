/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * For running {@link Worker}. Only used in in-memory/standalone mode.
 */
public class InMemoryWorkerRunner extends AbstractInMemoryProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryWorkerRunner.class);
  private static final Gson GSON = new Gson();

  @Inject
  InMemoryWorkerRunner(ProgramRunnerFactory programRunnerFactory) {
    super(programRunnerFactory);
  }

  @Override
  protected ProgramOptions createComponentOptions(String name, int instanceId, int instances, RunId runId,
                                                  ProgramOptions options) {
    Map<String, String> systemOptions = Maps.newHashMap();
    systemOptions.putAll(options.getArguments().asMap());
    systemOptions.put(ProgramOptionConstants.INSTANCE_ID, Integer.toString(instanceId));
    systemOptions.put(ProgramOptionConstants.INSTANCES, Integer.toString(instances));
    systemOptions.put(ProgramOptionConstants.RUN_ID, runId.getId());
    return new SimpleProgramOptions(name, new BasicArguments(systemOptions), options.getUserArguments());
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType type = program.getType();
    Preconditions.checkNotNull(type, "Missing processor type.");
    Preconditions.checkArgument(type == ProgramType.WORKER, "Only WORKER process type is supported.");

    WorkerSpecification workerSpec = appSpec.getWorkers().get(program.getName());
    Preconditions.checkNotNull(workerSpec, "Missing WorkerSpecification for %s", program.getName());

    String instances = options.getArguments().getOption(ProgramOptionConstants.INSTANCES,
                                                        String.valueOf(workerSpec.getInstances()));
    String resourceString = options.getArguments().getOption(ProgramOptionConstants.RESOURCES, null);
    Resources newResources = (resourceString != null) ? GSON.fromJson(resourceString, Resources.class) :
      workerSpec.getResources();

    WorkerSpecification newWorkerSpec = new WorkerSpecification(workerSpec.getClassName(), workerSpec.getName(),
                                                                workerSpec.getDescription(), workerSpec.getProperties(),
                                                                workerSpec.getDatasets(), newResources,
                                                                Integer.valueOf(instances));

    //RunId for worker
    RunId runId = RunIds.fromString(options.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    Table<String, Integer, ProgramController> components = startWorkers(program, runId, options, newWorkerSpec);
    return new InMemoryProgramController(components, runId, program, workerSpec, options,
                                         ProgramRunnerFactory.Type.WORKER_COMPONENT);
  }

  private Table<String, Integer, ProgramController> startWorkers(Program program, RunId runId, ProgramOptions options,
                                                                 WorkerSpecification spec) {
    Table<String, Integer, ProgramController> components = HashBasedTable.create();
    try {
      startComponent(program, program.getName(), spec.getInstances(), runId, options, components,
                     ProgramRunnerFactory.Type.WORKER_COMPONENT);
    } catch (Throwable t) {
      LOG.error("Failed to start all worker instances", t);
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
        LOG.error("Failed to stop all workers upon startup failure.", e);
        throw Throwables.propagate(e);
      }
    }
    return components;
  }
}

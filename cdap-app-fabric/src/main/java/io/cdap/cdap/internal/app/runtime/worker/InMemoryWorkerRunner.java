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

package io.cdap.cdap.internal.app.runtime.worker;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.worker.Worker;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.AbstractInMemoryProgramRunner;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.proto.ProgramType;

/**
 * For running {@link Worker}. Only used in in-memory/standalone mode.
 */
public class InMemoryWorkerRunner extends AbstractInMemoryProgramRunner {

  private final Provider<WorkerProgramRunner> workerProgramRunnerProvider;

  @Inject
  InMemoryWorkerRunner(CConfiguration cConf, Provider<WorkerProgramRunner> workerProgramRunnerProvider) {
    super(cConf);
    this.workerProgramRunnerProvider = workerProgramRunnerProvider;
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

    WorkerSpecification newWorkerSpec = new WorkerSpecification(workerSpec.getClassName(), workerSpec.getName(),
                                                                workerSpec.getDescription(), workerSpec.getProperties(),
                                                                workerSpec.getDatasets(), workerSpec.getResources(),
                                                                Integer.valueOf(instances), workerSpec.getPlugins());
    return startAll(program, options, newWorkerSpec.getInstances());
  }

  @Override
  protected ProgramRunner createProgramRunner() {
    return workerProgramRunnerProvider.get();
  }
}

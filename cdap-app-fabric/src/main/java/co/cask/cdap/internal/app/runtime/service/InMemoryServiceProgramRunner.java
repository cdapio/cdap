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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.AbstractInMemoryProgramRunner;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramStateChangeListener;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;

/**
 * For running {@link Service}. Only used in in-memory/standalone mode.
 */
public class InMemoryServiceProgramRunner extends AbstractInMemoryProgramRunner {

  private final Provider<ServiceProgramRunner> serviceProgramRunnerProvider;
  private final RuntimeStore runtimeStore;

  @Inject
  InMemoryServiceProgramRunner(CConfiguration cConf, Provider<ServiceProgramRunner> serviceProgramRunnerProvider,
                               RuntimeStore runtimeStore) {
    super(cConf);
    this.runtimeStore = runtimeStore;
    this.serviceProgramRunnerProvider = serviceProgramRunnerProvider;
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
    ProgramController controller = startAll(program, options, runId, serviceSpec.getInstances());
    Arguments systemArgs = options.getArguments();
    Arguments userArgs = options.getUserArguments();

    controller.addListener(
      new ProgramStateChangeListener(runtimeStore, program.getId(), runId, null, userArgs, systemArgs),
      Threads.SAME_THREAD_EXECUTOR);
    return controller;
  }

  @Override
  protected ProgramRunner createProgramRunner() {
    return serviceProgramRunnerProvider.get();
  }
}

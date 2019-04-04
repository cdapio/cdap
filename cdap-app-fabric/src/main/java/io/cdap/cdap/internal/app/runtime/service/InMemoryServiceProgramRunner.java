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

package io.cdap.cdap.internal.app.runtime.service;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.service.Service;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.AbstractInMemoryProgramRunner;
import io.cdap.cdap.proto.ProgramType;

/**
 * For running {@link Service}. Only used in in-memory/standalone mode.
 */
public class InMemoryServiceProgramRunner extends AbstractInMemoryProgramRunner {

  private final Provider<ServiceProgramRunner> serviceProgramRunnerProvider;

  @Inject
  InMemoryServiceProgramRunner(CConfiguration cConf, Provider<ServiceProgramRunner> serviceProgramRunnerProvider) {
    super(cConf);
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

    return startAll(program, options, serviceSpec.getInstances());
  }

  @Override
  protected ProgramRunner createProgramRunner() {
    return serviceProgramRunnerProvider.get();
  }
}

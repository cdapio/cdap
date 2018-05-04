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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.guice.ClusterMode;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;

import java.io.File;

/**
 * Distributed ProgramRunner for Service.
 */
public class DistributedServiceProgramRunner extends DistributedProgramRunner
                                             implements LongRunningDistributedProgramRunner {

  @Inject
  DistributedServiceProgramRunner(CConfiguration cConf, YarnConfiguration hConf,
                                  Impersonator impersonator, ClusterMode clusterMode,
                                  @Constants.AppFabric.ProgramRunner TwillRunner twillRunner) {
    super(cConf, hConf, impersonator, clusterMode, twillRunner);
  }

  @Override
  public ProgramController createProgramController(TwillController twillController,
                                                   ProgramDescriptor programDescriptor, RunId runId) {
    return new ServiceTwillProgramController(programDescriptor.getProgramId(), twillController, runId).startListen();
  }

  @Override
  protected void validateOptions(Program program, ProgramOptions options) {
    super.validateOptions(program, options);

    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.SERVICE, "Only SERVICE process type is supported.");

    ServiceSpecification serviceSpec = appSpec.getServices().get(program.getName());
    Preconditions.checkNotNull(serviceSpec, "Missing ServiceSpecification for %s", program.getName());
  }

  @Override
  protected void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program, ProgramOptions options,
                                   CConfiguration cConf, Configuration hConf, File tempDir) {

    ApplicationSpecification appSpec = program.getApplicationSpecification();
    ServiceSpecification serviceSpec = appSpec.getServices().get(program.getName());

    // Add a runnable for the service handler
    launchConfig.addRunnable(serviceSpec.getName(), new ServiceTwillRunnable(serviceSpec.getName()),
                             serviceSpec.getInstances(), options.getUserArguments().asMap(),
                             serviceSpec.getResources());
  }
}

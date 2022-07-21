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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;

/**
 * Distributed ProgramRunner for Service.
 */
public class DistributedServiceProgramRunner extends DistributedProgramRunner
                                             implements LongRunningDistributedProgramRunner {

  @Inject
  DistributedServiceProgramRunner(CConfiguration cConf, YarnConfiguration hConf,
                                  Impersonator impersonator, ClusterMode clusterMode,
                                  @Constants.AppFabric.ProgramRunner TwillRunner twillRunner, Injector injector) {
    super(cConf, hConf, impersonator, clusterMode, twillRunner, injector.getInstance(LocationFactory.class));
    if (!cConf.getBoolean(Constants.AppFabric.PROGRAM_REMOTE_RUNNER, false)) {
      this.namespaceQueryAdmin = injector.getInstance(NamespaceQueryAdmin.class);
    }
  }

  @Override
  public ProgramController createProgramController(ProgramRunId programRunId, TwillController twillController) {
    return new ServiceTwillProgramController(programRunId, twillController).startListen();
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

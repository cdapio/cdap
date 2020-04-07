/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;

import java.io.File;
import java.util.Collections;

/**
 * Distributed ProgramRunner for Worker.
 */
public class DistributedWorkerProgramRunner extends DistributedProgramRunner
                                            implements LongRunningDistributedProgramRunner {
  @Inject
  DistributedWorkerProgramRunner(CConfiguration cConf, YarnConfiguration hConf,
                                 Impersonator impersonator, ClusterMode clusterMode,
                                 @Constants.AppFabric.ProgramRunner TwillRunner twillRunner) {
    super(cConf, hConf, impersonator, clusterMode, twillRunner, null);
  }

  @Override
  public ProgramController createProgramController(TwillController twillController, ProgramId programId, RunId runId) {
    return new WorkerTwillProgramController(programId, twillController, runId).startListen();
  }

  @Override
  protected void validateOptions(Program program, ProgramOptions options) {
    super.validateOptions(program, options);

    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.WORKER, "Only WORKER process type is supported.");

    WorkerSpecification workerSpec = appSpec.getWorkers().get(program.getName());
    Preconditions.checkNotNull(workerSpec, "Missing WorkerSpecification for %s", program.getName());
  }

  @Override
  protected void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program, ProgramOptions options,
                                   CConfiguration cConf, Configuration hConf, File tempDir) {
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    WorkerSpecification workerSpec = appSpec.getWorkers().get(program.getName());

    String instances = options.getArguments().getOption(ProgramOptionConstants.INSTANCES,
                                                        String.valueOf(workerSpec.getInstances()));
    launchConfig.addRunnable(workerSpec.getName(), new WorkerTwillRunnable(workerSpec.getName()),
                             Integer.parseInt(instances), options.getUserArguments().asMap(),
                             workerSpec.getResources());
    if (clusterMode == ClusterMode.ISOLATED) {
      // For isolated mode, the hadoop classes comes from the hadoop classpath in the target cluster directly
      launchConfig.addExtraClasspath(Collections.singletonList("$HADOOP_CLASSPATH"));
    }
  }
}

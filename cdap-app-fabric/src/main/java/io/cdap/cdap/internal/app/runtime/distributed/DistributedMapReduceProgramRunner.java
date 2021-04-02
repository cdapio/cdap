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
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.YarnClientProtocolProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Runs MapReduce program in distributed environment
 */
public final class DistributedMapReduceProgramRunner extends DistributedProgramRunner {

  @Inject
  DistributedMapReduceProgramRunner(CConfiguration cConf, YarnConfiguration hConf,
                                    Impersonator impersonator, ClusterMode clusterMode,
                                    @Constants.AppFabric.ProgramRunner TwillRunner twillRunner) {
    super(cConf, hConf, impersonator, clusterMode, twillRunner);
  }

  @Override
  public ProgramController createProgramController(ProgramRunId programRunId, TwillController twillController) {
    return new MapReduceTwillProgramController(programRunId, twillController).startListen();
  }

  @Override
  protected void validateOptions(Program program, ProgramOptions options) {
    super.validateOptions(program, options);

    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.MAPREDUCE, "Only MapReduce process type is supported.");

    MapReduceSpecification spec = appSpec.getMapReduce().get(program.getName());
    Preconditions.checkNotNull(spec, "Missing MapReduceSpecification for %s", program.getName());
  }

  @Override
  protected void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program, ProgramOptions options,
                                   CConfiguration cConf, Configuration hConf, File tempDir) {
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    MapReduceSpecification spec = appSpec.getMapReduce().get(program.getName());

    // Get the resource for the container that runs the mapred client that will launch the actual mapred job.
    Map<String, String> clientArgs = RuntimeArguments.extractScope("task", "client",
                                                                   options.getUserArguments().asMap());
    // Add runnable. Only one instance for the MR driver
    launchConfig
      .addRunnable(spec.getName(), new MapReduceTwillRunnable(spec.getName()),
                   1, clientArgs, spec.getDriverResources(), 0);

    if (clusterMode == ClusterMode.ON_PREMISE || cConf.getBoolean(Constants.AppFabric.PROGRAM_REMOTE_RUNNER, false)) {
      // Add extra resources, classpath and dependencies
      launchConfig
        .addExtraResources(MapReduceContainerHelper.localizeFramework(hConf, new HashMap<>()))
        .addExtraClasspath(MapReduceContainerHelper.addMapReduceClassPath(hConf, new ArrayList<>()))
        .addExtraDependencies(YarnClientProtocolProvider.class);
    }
  }
}

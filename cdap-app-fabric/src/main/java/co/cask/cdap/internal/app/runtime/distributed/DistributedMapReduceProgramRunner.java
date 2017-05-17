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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.mapred.YarnClientProtocolProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
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
  DistributedMapReduceProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                    TokenSecureStoreRenewer tokenSecureStoreRenewer,
                                    Impersonator impersonator) {
    super(twillRunner, hConf, cConf, tokenSecureStoreRenewer, impersonator);
  }

  @Override
  public ProgramController createProgramController(TwillController twillController,
                                                   ProgramDescriptor programDescriptor, RunId runId) {

    return new MapReduceTwillProgramController(programDescriptor.getProgramId(), twillController, runId).startListen();
  }

  @Override
  protected Map<String, ProgramTwillApplication.RunnableResource> getRunnables(Program program,
                                                                               ProgramOptions programOptions) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.MAPREDUCE, "Only MapReduce process type is supported.");

    MapReduceSpecification spec = appSpec.getMapReduce().get(program.getName());
    Preconditions.checkNotNull(spec, "Missing MapReduceSpecification for %s", program.getName());

    // Get the resource for the container that runs the mapred client that will launch the actual mapred job.
    Map<String, String> clientArgs = RuntimeArguments.extractScope("task", "client",
                                                                   programOptions.getUserArguments().asMap());
    Resources resources = SystemArguments.getResources(clientArgs, spec.getDriverResources());

    Map<String, ProgramTwillApplication.RunnableResource> runnables = new HashMap<>();
    runnables.put(spec.getName(),
                  new ProgramTwillApplication.RunnableResource(new MapReduceTwillRunnable(spec.getName()),
                                                               createResourceSpec(resources, 1)));
    return runnables;
  }

  @Override
  protected Map<String, LocalizeResource> getExtraLocalizeResources(Program program, File tempDir) {
    Map<String, LocalizeResource> localizeResources = new HashMap<>();
    MapReduceContainerHelper.localizeFramework(hConf, localizeResources);
    return localizeResources;
  }

  @Override
  protected void prepareLaunch(Program program, TwillPreparer preparer) {
    preparer
      .withClassPaths(MapReduceContainerHelper.addMapReduceClassPath(hConf, new ArrayList<String>()))
      .withDependencies(YarnClientProtocolProvider.class);
  }
}

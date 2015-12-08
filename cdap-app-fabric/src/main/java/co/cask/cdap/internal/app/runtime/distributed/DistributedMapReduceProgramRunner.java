/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Runs Mapreduce programm in distributed environment
 */
public final class DistributedMapReduceProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedMapReduceProgramRunner.class);

  @Inject
  public DistributedMapReduceProgramRunner(TwillRunner twillRunner, LocationFactory locationFactory,
                                           YarnConfiguration hConf, CConfiguration cConf) {
    super(twillRunner, locationFactory, hConf, cConf);
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options,
                                     Map<String, LocalizeResource> localizeResources,
                                     final ApplicationLauncher launcher) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.MAPREDUCE, "Only MAPREDUCE process type is supported.");

    MapReduceSpecification spec = appSpec.getMapReduce().get(program.getName());
    Preconditions.checkNotNull(spec, "Missing MapReduceSpecification for %s", program.getName());

    List<String> extraClassPaths = MapReduceContainerHelper.localizeFramework(hConf, localizeResources);

    // TODO(CDAP-3119): Hack for TWILL-144. Need to remove
    File launcherFile = null;
    if (MapReduceContainerHelper.getFrameworkURI(hConf) != null) {
      File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                              cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
      tempDir.mkdirs();
      try {
        launcherFile = File.createTempFile("launcher", ".jar", tempDir);
        MapReduceContainerHelper.saveLauncher(hConf, launcherFile, extraClassPaths);
        localizeResources.put("launcher.jar", new LocalizeResource(launcherFile));
      } catch (Exception e) {
        LOG.warn("Failed to create twill container launcher.jar for TWILL-144 hack. " +
                   "Still proceed, but the run will likely fail", e);
      }
    }
    // End Hack for TWILL-144

    LOG.info("Launching MapReduce program: " + program.getName() + ":" + spec.getName());
    TwillController controller = launcher.launch(
      new MapReduceTwillApplication(program, spec, localizeResources, eventHandler),
      extraClassPaths);

    // TODO(CDAP-3119): Hack for TWILL-144. Need to remove
    final File cleanupFile = launcherFile;
    Runnable cleanupTask = new Runnable() {
      @Override
      public void run() {
        if (cleanupFile != null) {
          cleanupFile.delete();
        }
      }
    };
    // Cleanup when the app is running. Also add a safe guide to do cleanup on terminate in case there is race
    // such that the app terminated before onRunning was called
    controller.onRunning(cleanupTask, Threads.SAME_THREAD_EXECUTOR);
    controller.onTerminated(cleanupTask, Threads.SAME_THREAD_EXECUTOR);
    // End Hack for TWILL-144

    RunId runId = RunIds.fromString(options.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    return new MapReduceTwillProgramController(program.getId(), controller, runId).startListen();
  }
}

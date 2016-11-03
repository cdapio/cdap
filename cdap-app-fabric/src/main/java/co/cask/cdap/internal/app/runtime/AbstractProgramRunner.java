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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.NameMappedDatasetFramework;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Provides method to create {@link PluginInstantiator} for Program Runners
 */
public abstract class AbstractProgramRunner implements ProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramRunner.class);

  private final CConfiguration cConf;
  private final RuntimeStore runtimeStore;
  private final DatasetFramework datasetFramework;

  public AbstractProgramRunner(CConfiguration cConf, RuntimeStore runtimeStore,
                               DatasetFramework datasetFramework) {
    this.cConf = cConf;
    this.runtimeStore = runtimeStore;
    this.datasetFramework = datasetFramework;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {

    final Arguments arguments = options.getArguments();
    final Arguments userArgs = options.getUserArguments();
    final RunId runId = ProgramRunners.getRunId(options);
    final ProgramId programId = program.getId();
    final String twillRunId = arguments.getOption(ProgramOptionConstants.TWILL_RUN_ID);
    final Deque<Closeable> closeables = new LinkedList<>();

    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    // Get the WorkflowProgramInfo if it is started by Workflow
    WorkflowProgramInfo workflowInfo = WorkflowProgramInfo.create(arguments);
    DatasetFramework programDatasetFramework = workflowInfo == null ?
      datasetFramework :
      NameMappedDatasetFramework.createFromWorkflowProgramInfo(datasetFramework, workflowInfo, appSpec);

    // Setup dataset framework context, if required
    if (programDatasetFramework instanceof ProgramContextAware) {
      ((ProgramContextAware) programDatasetFramework).initContext(programId.run(runId));
    }

    PluginInstantiator pluginInstantiator = null;
    if (arguments.hasOption(ProgramOptionConstants.PLUGIN_DIR)) {
      pluginInstantiator = new PluginInstantiator(
        cConf, program.getClassLoader(), new File(arguments.getOption(ProgramOptionConstants.PLUGIN_DIR)));
      closeables.add(pluginInstantiator);
    }

    try {
      final ProgramController controller = startProgram(program, options, runId,
                                                        programDatasetFramework, pluginInstantiator, workflowInfo,
                                                        closeables);

      controller.addListener(
        new AbstractListener() {
          @Override
          public void init(ProgramController.State state, @Nullable Throwable cause) {
            // Get start time from RunId
            long startTimeInSeconds = RunIds.getTime(controller.getRunId(), TimeUnit.SECONDS);
            if (startTimeInSeconds == -1) {
              // If RunId is not time-based, use current time as start time
              startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            }
            runtimeStore.setStart(programId, runId.getId(), startTimeInSeconds, twillRunId,
                                  userArgs.asMap(), arguments.asMap());
            if (state == ProgramController.State.COMPLETED) {
              completed();
            }
            if (state == ProgramController.State.ERROR) {
              error(controller.getFailureCause());
            }
          }

          @Override
          public void completed() {
            LOG.debug("Program {} completed successfully.", programId);
            closeAllQuietly(closeables);
            runtimeStore.setStop(programId, runId.getId(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                 ProgramController.State.COMPLETED.getRunStatus());
          }

          @Override
          public void killed() {
            LOG.debug("Program {} killed.", programId);
            closeAllQuietly(closeables);
            runtimeStore.setStop(programId, runId.getId(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                 ProgramController.State.KILLED.getRunStatus());
          }

          @Override
          public void suspended() {
            LOG.debug("Suspending Program {} {}.", programId, runId);
            runtimeStore.setSuspend(programId, runId.getId());
          }

          @Override
          public void resuming() {
            LOG.debug("Resuming Program {} {}.", programId, runId);
            runtimeStore.setResume(programId, runId.getId());
          }

          @Override
          public void error(Throwable cause) {
            LOG.info("Program stopped with error {}, {}", programId, runId, cause);
            closeAllQuietly(closeables);
            runtimeStore.setStop(programId, runId.getId(), TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                 ProgramController.State.ERROR.getRunStatus(), new BasicThrowable(cause));
          }
        },
        Threads.SAME_THREAD_EXECUTOR);
      return controller;
    } catch (Throwable t) {
      closeAllQuietly(closeables);
      throw Throwables.propagate(t);
    }
  }

  /**
   * Create the ProgramController for the program and start it.
   *
   * @param program the program to start
   * @param options the program options
   * @param runId the program run id
   * @param datasetFramework the program dataset framework
   * @param pluginInstantiator the plugin instantiator for the program, or null if it does not exist
   * @param workflowInfo the workflow information if the program is part of a workflow run, or null if it is not
   *                     part of a workflow run
   * @param closeables implementations should add any resources that need to be closed at program end
   * @return the controller used to start the program
   */
  protected abstract ProgramController startProgram(Program program,
                                                    ProgramOptions options,
                                                    RunId runId,
                                                    DatasetFramework datasetFramework,
                                                    @Nullable PluginInstantiator pluginInstantiator,
                                                    @Nullable WorkflowProgramInfo workflowInfo,
                                                    Deque<Closeable> closeables) throws Exception;

  protected void closeAllQuietly(Iterable<Closeable> closeables) {
    for (Closeable closeable : closeables) {
      Closeables.closeQuietly(closeable);
    }
  }

  @Nullable
  protected File getPluginArchive(ProgramOptions options) {
    if (!options.getArguments().hasOption(ProgramOptionConstants.PLUGIN_ARCHIVE)) {
      return null;
    }
    return new File(options.getArguments().getOption(ProgramOptionConstants.PLUGIN_ARCHIVE));
  }
}

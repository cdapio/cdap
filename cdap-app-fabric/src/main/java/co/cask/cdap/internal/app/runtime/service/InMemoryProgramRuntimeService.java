/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.AbstractProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.async.ExecutorUtils;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.InMemoryProgramLiveInfo;
import co.cask.cdap.proto.NotRunningProgramLiveInfo;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public final class InMemoryProgramRuntimeService extends AbstractProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryProgramRuntimeService.class);

  private final CConfiguration cConf;

  @Inject
  public InMemoryProgramRuntimeService(ProgramRunnerFactory programRunnerFactory, CConfiguration cConf) {
    super(programRunnerFactory);
    this.cConf = cConf;
  }

  @Override
  public synchronized RuntimeInfo run(Program program, ProgramOptions options) {
    try {
      File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR), cConf.get(Constants.AppFabric.TEMP_DIR));
      final File destinationUnpackedJarDir = new File(tmpDir, String.format("%s.%s",
                                                                      program.getName(), UUID.randomUUID().toString()));
      Preconditions.checkState(!destinationUnpackedJarDir.exists());
      destinationUnpackedJarDir.mkdirs();

      Program bundleJarProgram = Programs.createWithUnpack(program.getJarLocation(), destinationUnpackedJarDir);
      RuntimeInfo info = super.run(bundleJarProgram, options);
      final ProgramController controller = info.getController();
      controller.addListener(new AbstractListener() {

        @Override
        public void init(ProgramController.State state) {
          if (state == ProgramController.State.COMPLETED) {
            completed();
          }
          if (state == ProgramController.State.ERROR) {
            error(controller.getFailureCause());
          }
        }


        @Override
        public void killed() {
          try {
            FileUtils.deleteDirectory(destinationUnpackedJarDir);
          } catch (IOException e) {
            LOG.warn("Failed to cleanup temporary program directory {}.", destinationUnpackedJarDir, e);
          }
        }

        @Override
        public void completed() {
          try {
            FileUtils.deleteDirectory(destinationUnpackedJarDir);
          } catch (IOException e) {
            LOG.warn("Failed to cleanup temporary program directory {}.", destinationUnpackedJarDir, e);
          }
        }

        @Override
        public void error(Throwable cause) {
          try {
            FileUtils.deleteDirectory(destinationUnpackedJarDir);
          } catch (IOException e) {
            LOG.warn("Failed to cleanup temporary program directory {}.", destinationUnpackedJarDir, e);
          }
        }
      }, ExecutorUtils.newThreadExecutor(Threads.createDaemonThreadFactory("program-clean-up-%d")));

      return info;

    } catch (IOException e) {
      throw new RuntimeException("Error unpackaging program " + program.getName());
    }
  }

  @Override
  public ProgramLiveInfo getLiveInfo(Id.Program programId, ProgramType type) {
    return isRunning(programId, type) ? new InMemoryProgramLiveInfo(programId, type)
                                      : new NotRunningProgramLiveInfo(programId, type);
  }

  @Override
  protected void shutDown() throws Exception {
    stopAllPrograms();
  }

  private void stopAllPrograms() {

    LOG.info("Stopping all running programs.");

    List<ListenableFuture<ProgramController>> futures = Lists.newLinkedList();
    for (ProgramType type : ProgramType.values()) {
      for (Map.Entry<RunId, RuntimeInfo> entry : list(type).entrySet()) {
        RuntimeInfo runtimeInfo = entry.getValue();
        if (isRunning(runtimeInfo.getProgramId(), type)) {
          futures.add(runtimeInfo.getController().stop());
        }
      }
    }
    // unchecked because we cannot do much if it fails. We will still shutdown the standalone CDAP instance.
    try {
      Futures.successfulAsList(futures).get(60, TimeUnit.SECONDS);
      LOG.info("All programs have been stopped.");
    } catch (ExecutionException e) {
      // note this should not happen because we wait on a successfulAsList
      LOG.warn("Got exception while waiting for all programs to stop", e.getCause());
    } catch (InterruptedException e) {
      LOG.warn("Got interrupted exception while waiting for all programs to stop", e);
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      // can't do much more than log it. We still want to exit.
      LOG.warn("Timeout while waiting for all programs to stop.");
    }
  }
}

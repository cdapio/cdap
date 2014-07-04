package com.continuuity.internal.app.runtime.service;

import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.AbstractProgramRuntimeService;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public final class InMemoryProgramRuntimeService extends AbstractProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryProgramRuntimeService.class);

  private final CConfiguration configuration;

  @Inject
  public InMemoryProgramRuntimeService(ProgramRunnerFactory programRunnerFactory,
                                       CConfiguration configuration) {
    super(programRunnerFactory);
    this.configuration = configuration;
  }

  @Override
  public synchronized RuntimeInfo run(Program program, ProgramOptions options) {
    try {
      // TODO: fix possible issue where two run() calls use the same unpackedLocation
      File destinationUnpackedJarDir = new File(
        configuration.get(Constants.AppFabric.TEMP_DIR) + "/" + program.getName() + "-" + System.currentTimeMillis());
      Preconditions.checkState(!destinationUnpackedJarDir.exists());
      destinationUnpackedJarDir.mkdirs();

      Program bundleJarProgram = Programs.createWithUnpack(program.getJarLocation(), destinationUnpackedJarDir);
      return super.run(bundleJarProgram, options);
    } catch (IOException e) {
      throw new RuntimeException("Error unpackaging program " + program.getName());
    }
  }

  @Override
  public LiveInfo getLiveInfo(Id.Program programId, Type type) {
    return isRunning(programId, type)
      ? new InMemoryLiveInfo(programId, type)
      : new NotRunningLiveInfo(programId, type);
  }

  @Override
  protected void shutDown() throws Exception {
    stopAllPrograms();
  }

  private void stopAllPrograms() {

    LOG.info("Stopping all running programs.");

    List<ListenableFuture<ProgramController>> futures = Lists.newLinkedList();
    for (Type type : Type.values()) {
      for (Map.Entry<RunId, RuntimeInfo> entry : list(type).entrySet()) {
        RuntimeInfo runtimeInfo = entry.getValue();
        if (isRunning(runtimeInfo.getProgramId(), type)) {
          futures.add(runtimeInfo.getController().stop());
        }
      }
    }
    // unchecked because we cannot do much if it fails. We will still shutdown the singlenode.
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

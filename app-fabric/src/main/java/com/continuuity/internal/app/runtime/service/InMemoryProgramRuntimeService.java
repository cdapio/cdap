package com.continuuity.internal.app.runtime.service;

import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.AbstractProgramRuntimeService;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 *
 */
public final class InMemoryProgramRuntimeService extends AbstractProgramRuntimeService {

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

      Program bundleJarProgram = Programs.create(program.getJarLocation(), destinationUnpackedJarDir);
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
}

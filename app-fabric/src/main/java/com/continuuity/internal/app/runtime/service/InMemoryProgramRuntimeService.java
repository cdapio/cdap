package com.continuuity.internal.app.runtime.service;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.AbstractProgramRuntimeService;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.google.inject.Inject;

/**
 *
 */
public final class InMemoryProgramRuntimeService extends AbstractProgramRuntimeService {

  @Inject
  public InMemoryProgramRuntimeService(ProgramRunnerFactory programRunnerFactory) {
    super(programRunnerFactory);
  }

  @Override
  public LiveInfo getLiveInfo(Id.Program programId, Type type) {
    return isRunning(programId, type)
      ? new InMemoryLiveInfo(programId, type)
      : new NotRunningLiveInfo(programId, type);
  }
}

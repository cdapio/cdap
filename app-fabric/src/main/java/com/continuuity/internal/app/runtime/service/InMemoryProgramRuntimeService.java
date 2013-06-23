package com.continuuity.internal.app.runtime.service;

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
}

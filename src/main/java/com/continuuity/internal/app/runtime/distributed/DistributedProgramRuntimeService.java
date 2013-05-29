/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.runtime.RunId;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.service.SimpleRuntimeInfo;
import com.continuuity.weave.api.WeaveRunner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

import java.util.Map;

/**
 *
 */
public final class DistributedProgramRuntimeService extends AbstractIdleService implements ProgramRuntimeService {

  private final ProgramRunnerFactory programRunnerFactory;
  private final WeaveRunner weaveRunner;

  @Inject
  DistributedProgramRuntimeService(ProgramRunnerFactory programRunnerFactory, WeaveRunner weaveRunner) {
    this.programRunnerFactory = programRunnerFactory;
    this.weaveRunner = weaveRunner;
  }


  @Override
  protected void startUp() throws Exception {
    // No-op
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  @Override
  public RuntimeInfo run(Program program, ProgramOptions options) {
    ProgramRunner runner = null;

    switch (program.getProcessorType()) {
      case FLOW:
        runner = programRunnerFactory.create(ProgramRunnerFactory.Type.FLOW);
        break;
      case PROCEDURE:
        runner = programRunnerFactory.create(ProgramRunnerFactory.Type.PROCEDURE);
        break;
      case MAPREDUCE:
        runner = programRunnerFactory.create(ProgramRunnerFactory.Type.MAPREDUCE);
        break;
    }

    Preconditions.checkNotNull(runner, "Fail to get ProgramRunner for type " + program.getProcessorType());
    return new SimpleRuntimeInfo(runner.run(program, options), program);
  }

  @Override
  public RuntimeInfo lookup(RunId runId) {
    // TODO (ENG-2526)
    return null;
  }

  @Override
  public Map<RunId, RuntimeInfo> list(Type type) {
    // TODO (ENG-2526)
    return ImmutableMap.of();
  }
}

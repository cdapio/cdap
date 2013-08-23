/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.runtime;

import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.service.SimpleRuntimeInfo;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.common.Threads;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A ProgramRuntimeService that keeps an in memory map for all running programs.
 */
public abstract class AbstractProgramRuntimeService extends AbstractIdleService implements ProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramRuntimeService.class);

  private final Table<Type, RunId, RuntimeInfo> runtimeInfos;
  private final ProgramRunnerFactory programRunnerFactory;

  protected AbstractProgramRuntimeService(ProgramRunnerFactory programRunnerFactory) {
    this.runtimeInfos = HashBasedTable.create();
    this.programRunnerFactory = programRunnerFactory;
  }

  @Override
  public final synchronized RuntimeInfo run(Program program, ProgramOptions options) {
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
    final SimpleRuntimeInfo runtimeInfo = new SimpleRuntimeInfo(runner.run(program, options), program);
    addRemover(runtimeInfo);
    runtimeInfos.put(runtimeInfo.getType(), runtimeInfo.getController().getRunId(), runtimeInfo);
    return runtimeInfo;
  }

  @Override
  public synchronized RuntimeInfo lookup(RunId runId) {
    Map<Type, RuntimeInfo> column = runtimeInfos.column(runId);
    if (column.size() != 1) {
      // It should be exactly one if the the program is running.
      return null;
    }
    return column.values().iterator().next();
  }

  @Override
  public synchronized Map<RunId, RuntimeInfo> list(Type type) {
    return ImmutableMap.copyOf(runtimeInfos.row(type));
  }

  @Override
  protected void startUp() throws Exception {
    // No-op
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  protected synchronized void updateRuntimeInfo(Type type, RunId runId, RuntimeInfo runtimeInfo) {
    if (!runtimeInfos.contains(type, runId)) {
      runtimeInfos.put(type, runId, addRemover(runtimeInfo));
    }
  }

  private RuntimeInfo addRemover(final RuntimeInfo runtimeInfo) {
    runtimeInfo.getController().addListener(new AbstractListener() {
      @Override
      public void stopped() {
        remove(runtimeInfo);
      }

      @Override
      public void error(Throwable cause) {
        remove(runtimeInfo);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return runtimeInfo;
  }

  private synchronized void remove(RuntimeInfo info) {
    LOG.debug("Removing RuntimeInfo: {} {} {}",
              info.getType(), info.getProgramId().getId(), info.getController().getRunId());
    RuntimeInfo removed = runtimeInfos.remove(info.getType(), info.getController().getRunId());
    LOG.debug("RuntimeInfo removed: {}", removed);
  }
}

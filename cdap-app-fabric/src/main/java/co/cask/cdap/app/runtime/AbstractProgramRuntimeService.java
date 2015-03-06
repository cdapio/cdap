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
package co.cask.cdap.app.runtime;

import co.cask.cdap.app.program.Program;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.service.SimpleRuntimeInfo;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A ProgramRuntimeService that keeps an in memory map for all running programs.
 */
public abstract class AbstractProgramRuntimeService extends AbstractIdleService implements ProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramRuntimeService.class);

  private final Table<ProgramType, RunId, RuntimeInfo> runtimeInfos;
  private final ProgramRunnerFactory programRunnerFactory;

  protected AbstractProgramRuntimeService(ProgramRunnerFactory programRunnerFactory) {
    this.runtimeInfos = HashBasedTable.create();
    this.programRunnerFactory = programRunnerFactory;
  }

  @Override
  public synchronized RuntimeInfo run(Program program, ProgramOptions options) {
    ProgramRunner runner = programRunnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
    Preconditions.checkNotNull(runner, "Fail to get ProgramRunner for type " + program.getType());
    final SimpleRuntimeInfo runtimeInfo = new SimpleRuntimeInfo(runner.run(program, options), program);
    addRemover(runtimeInfo);
    runtimeInfos.put(runtimeInfo.getType(), runtimeInfo.getController().getRunId(), runtimeInfo);
    return runtimeInfo;
  }

  @Override
  public synchronized RuntimeInfo lookup(RunId runId) {
    Map<ProgramType, RuntimeInfo> column = runtimeInfos.column(runId);
    if (column.size() != 1) {
      // It should be exactly one if the the program is running.
      return null;
    }
    return column.values().iterator().next();
  }

  @Override
  public synchronized Map<RunId, RuntimeInfo> list(ProgramType type) {
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

  protected synchronized void updateRuntimeInfo(ProgramType type, RunId runId, RuntimeInfo runtimeInfo) {
    if (!runtimeInfos.contains(type, runId)) {
      runtimeInfos.put(type, runId, addRemover(runtimeInfo));
    }
  }

  private RuntimeInfo addRemover(final RuntimeInfo runtimeInfo) {
    final ProgramController controller = runtimeInfo.getController();
    controller.addListener(new AbstractListener() {

      @Override
      public void completed() {
        remove(runtimeInfo);
      }

      @Override
      public void killed() {
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

  protected boolean isRunning(Id.Program programId, ProgramType type) {
    for (Map.Entry<RunId, RuntimeInfo> entry : list(type).entrySet()) {
      if (entry.getValue().getProgramId().equals(programId)) {
        return true;
      }
    }
    return false;
  }
}

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
package co.cask.cdap.app.runtime;

import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.service.SimpleRuntimeInfo;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;

/**
 * A ProgramRuntimeService that keeps an in memory map for all running programs.
 */
public abstract class AbstractProgramRuntimeService extends AbstractIdleService implements ProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramRuntimeService.class);
  private static final EnumSet<ProgramController.State> COMPLETED_STATES = EnumSet.of(ProgramController.State.COMPLETED,
                                                                                      ProgramController.State.KILLED,
                                                                                      ProgramController.State.ERROR);

  private final ReadWriteLock runtimeInfosLock;
  private final Table<ProgramType, RunId, RuntimeInfo> runtimeInfos;
  private final ProgramRunnerFactory programRunnerFactory;

  protected AbstractProgramRuntimeService(ProgramRunnerFactory programRunnerFactory) {
    this.runtimeInfosLock = new ReentrantReadWriteLock();
    this.runtimeInfos = HashBasedTable.create();
    this.programRunnerFactory = programRunnerFactory;
  }

  @Override
  public RuntimeInfo run(Program program, ProgramOptions options) {
    ProgramRunner runner = programRunnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
    Preconditions.checkNotNull(runner, "Fail to get ProgramRunner for type " + program.getType());
    ProgramOptions optionsWithRunId = addRunId(options, RunIds.generate());
    final RuntimeInfo runtimeInfo = createRuntimeInfo(runner.run(program, optionsWithRunId), program);
    programStarted(runtimeInfo);
    return runtimeInfo;
  }

  /**
   * Return the copy of the {@link ProgramOptions} including RunId in it.
   * @param options The {@link ProgramOptions} in which the RunId to be included
   * @param runId   The RunId to be included
   * @return the copy of the program options with RunId included in them
   */
  private ProgramOptions addRunId(ProgramOptions options, RunId runId) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options.getArguments().asMap());
    builder.put(ProgramOptionConstants.RUN_ID, runId.getId());

    return new SimpleProgramOptions(options.getName(), new BasicArguments(builder.build()), options.getUserArguments(),
                                    options.isDebug());
  }

  protected RuntimeInfo createRuntimeInfo(ProgramController controller, Program program) {
    return new SimpleRuntimeInfo(controller, program);
  }

  protected List<RuntimeInfo> getRuntimeInfos() {
    Lock lock = runtimeInfosLock.readLock();
    lock.lock();
    try {
      return ImmutableList.copyOf(runtimeInfos.values());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public RuntimeInfo lookup(Id.Program programId, RunId runId) {
    Lock lock = runtimeInfosLock.readLock();
    lock.lock();
    try {
      return runtimeInfos.get(programId.getType(), runId);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Map<RunId, RuntimeInfo> list(ProgramType type) {
    Lock lock = runtimeInfosLock.readLock();
    lock.lock();
    try {
      return ImmutableMap.copyOf(runtimeInfos.row(type));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Map<RunId, RuntimeInfo> list(final Id.Program program) {
    Lock lock = runtimeInfosLock.readLock();
    lock.lock();
    try {
      return Maps.filterValues(list(program.getType()), new Predicate<RuntimeInfo>() {
        @Override
        public boolean apply(RuntimeInfo info) {
          return info.getProgramId().equals(program);
        }
      });
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean checkAnyRunning(Predicate<Id.Program> predicate, ProgramType... types) {
    for (ProgramType type : types) {
      for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry :  list(type).entrySet()) {
        ProgramController.State programState = entry.getValue().getController().getState();
        if (programState.isDone()) {
          continue;
        }
        Id.Program programId = entry.getValue().getProgramId();
        if (predicate.apply(programId)) {
          LOG.trace("Program still running in checkAnyRunning: {} {} {} {}",
                    programId.getApplicationId(), type, programId.getId(), entry.getValue().getController().getRunId());
          return true;
        }
      }
    }
    return false;
  }

  @Override
  protected void startUp() throws Exception {
    // No-op
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  protected void updateRuntimeInfo(ProgramType type, RunId runId, RuntimeInfo runtimeInfo) {
    Lock lock = runtimeInfosLock.writeLock();
    lock.lock();
    try {
      if (!runtimeInfos.contains(type, runId)) {
        runtimeInfos.put(type, runId, programStarted(runtimeInfo));
      }
    } finally {
      lock.unlock();
    }
  }

  private RuntimeInfo programStarted(final RuntimeInfo runtimeInfo) {
    final ProgramController controller = runtimeInfo.getController();
    controller.addListener(new AbstractListener() {

      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        if (!COMPLETED_STATES.contains(currentState)) {
          add(runtimeInfo);
        }
      }

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

  private void add(RuntimeInfo runtimeInfo) {
    Lock lock = runtimeInfosLock.writeLock();
    lock.lock();
    try {
      runtimeInfos.put(runtimeInfo.getType(), runtimeInfo.getController().getRunId(), runtimeInfo);
    } finally {
      lock.unlock();
    }
  }

  private void remove(RuntimeInfo info) {
    Lock lock = runtimeInfosLock.writeLock();
    lock.lock();
    try {
      LOG.debug("Removing RuntimeInfo: {} {} {}",
                info.getType(), info.getProgramId().getId(), info.getController().getRunId());
      RuntimeInfo removed = runtimeInfos.remove(info.getType(), info.getController().getRunId());
      LOG.debug("RuntimeInfo removed: {}", removed);
    } finally {
      lock.unlock();
    }
  }

  protected boolean isRunning(Id.Program programId) {
    for (Map.Entry<RunId, RuntimeInfo> entry : list(programId.getType()).entrySet()) {
      if (entry.getValue().getProgramId().equals(programId)) {
        return true;
      }
    }
    return false;
  }
}

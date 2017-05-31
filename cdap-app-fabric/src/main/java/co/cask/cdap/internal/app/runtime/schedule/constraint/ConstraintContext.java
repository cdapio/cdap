/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.constraint;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.internal.app.runtime.schedule.queue.Job;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Context object, exposing information that may be useful during checking constraints.
 */
public final class ConstraintContext {
  private final Job job;
  private final long checkTime;
  private final Store store;

  public ConstraintContext(Job job, long checkTime, Store store) {
    this.job = job;
    this.checkTime = checkTime;
    this.store = store;
  }

  public long getCheckTime() {
    return checkTime;
  }

  public Map<ProgramRunId, RunRecordMeta> getProgramRuns(ProgramId programId, ProgramRunStatus status, int limit) {
    return store.getRuns(programId, status, 0, Long.MAX_VALUE, limit);
  }

  public Map<ProgramRunId, RunRecordMeta> getProgramRuns(ProgramId programId, ProgramRunStatus status,
                                                         long startTime, long endTime, int limit) {
    return store.getRuns(programId, status, startTime, endTime, limit);
  }

  public Job getJob() {
    return job;
  }
}

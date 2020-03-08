/*
 * Copyright © 2017-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.constraint;

import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;

import java.util.Map;

/**
 * Context object, exposing information that may be useful during checking constraints.
 */
public final class ConstraintContext {
  private final Job job;
  private final long checkTimeMillis;
  private final Store store;

  public ConstraintContext(Job job, long checkTimeMillis, Store store) {
    this.job = job;
    this.checkTimeMillis = checkTimeMillis;
    this.store = store;
  }

  public long getCheckTimeMillis() {
    return checkTimeMillis;
  }

  public Map<ProgramRunId, RunRecordDetail> getActiveRuns(ProgramId programId) {
    return store.getActiveRuns(programId);
  }

  public Map<ProgramRunId, RunRecordDetail> getProgramRuns(ProgramId programId, ProgramRunStatus status,
                                                           long startTime, long endTime, int limit) {
    return store.getRuns(programId, status, startTime, endTime, limit);
  }

  public Job getJob() {
    return job;
  }
}

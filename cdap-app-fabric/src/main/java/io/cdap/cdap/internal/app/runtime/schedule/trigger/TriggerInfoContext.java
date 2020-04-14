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

package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Context object, exposing information that may be useful during the construction of
 * {@link io.cdap.cdap.api.schedule.TriggerInfo} for a trigger.
 */
public class TriggerInfoContext {

  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  private final Job job;
  private final Store store;

  public TriggerInfoContext(Job job, Store store) {
    this.job = job;
    this.store = store;
  }

  /**
   * @return The {@link ProgramSchedule} which contains the trigger.
   */
  public ProgramSchedule getSchedule() {
    return job.getSchedule();
  }

  /**
   * @return A list of notifications which satisfy the trigger.
   */
  public List<Notification> getNotifications() {
    return job.getNotifications();
  }

  public ApplicationSpecification getApplicationSpecification(ApplicationId applicationId) {
    return store.getApplication(applicationId);
  }

  /**
   * @return The workflow token if the program is a workflow, {@code null} otherwise.
   */
  @Nullable
  public WorkflowToken getWorkflowToken(ProgramRunId programRunId) {
    ProgramId programId = programRunId.getParent();
    if (!programId.getType().equals(ProgramType.WORKFLOW)) {
      return null;
    }
    return store.getWorkflowToken(new WorkflowId(programId.getParent(), programId.getProgram()), programRunId.getRun());
  }

  /**
   * Fetches the run time arguments in a run record for a particular run of a program.
   *
   * @param programRunId The program run id
   * @return run time arguments as a map for the specified program and runId, null if not found
   */
  public Map<String, String> getProgramRuntimeArguments(ProgramRunId programRunId) {
    RunRecordDetail runRecordMeta = store.getRun(programRunId);
    if (runRecordMeta == null) {
      return Collections.emptyMap();
    }
    return runRecordMeta.getUserArgs();
  }
}

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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.queue.Job;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.WorkflowId;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Context object, exposing information that may be useful during the construction of
 * {@link co.cask.cdap.api.schedule.TriggerInfo} for a trigger.
 */
public class TriggerInfoContext {
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
  public WorkflowToken getWorkflowToken(ProgramId programId, String runId) {
    if (!programId.getType().equals(ProgramType.WORKFLOW)) {
      return null;
    }
    return store.getWorkflowToken(new WorkflowId(programId.getParent(), programId.getProgram()), runId);
  }

  /**
   * Fetches the run time arguments in a run record for a particular run of a program.
   *
   * @param programId id of the program
   * @param runId run id of the program
   * @return run time arguments as a map for the specified program and runId, null if not found
   */
  @Nullable
  public Map<String, String> getProgramRuntimeArguments(ProgramId programId, String runId) {
    RunRecordMeta runRecordMeta = store.getRun(programId, runId);
    if (runRecordMeta == null) {
      return null;
    }
    return runRecordMeta.getProperties();
  }
}

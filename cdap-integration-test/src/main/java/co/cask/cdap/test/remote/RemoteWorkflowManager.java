/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test.remote;

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.AbstractProgramManager;
import co.cask.cdap.test.ScheduleManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.base.Throwables;

import java.util.List;

/**
 * Remote implementation of {@link WorkflowManager}.
 */
public class RemoteWorkflowManager extends AbstractProgramManager<WorkflowManager> implements WorkflowManager {

  private final ScheduleClient scheduleClient;
  private final ProgramClient programClient;
  private final Id.Workflow workflowId;

  public RemoteWorkflowManager(Id.Workflow programId, ClientConfig clientConfig, RESTClient restClient,
                               RemoteApplicationManager applicationManager) {
    super(programId, applicationManager);
    this.workflowId = programId;
    this.programClient = new ProgramClient(clientConfig, restClient);
    this.scheduleClient = new ScheduleClient(clientConfig, restClient);
  }

  @Override
  public List<ScheduleSpecification> getSchedules() {
    try {
      return scheduleClient.list(workflowId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<RunRecord> getHistory() {
    try {
      return programClient.getProgramRuns(workflowId, "ALL", 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public ScheduleManager getSchedule(final String schedName) {
    return new ScheduleManager() {
      @Override
      public void suspend() {
        try {
          scheduleClient.suspend(Id.Schedule.from(programId.getApplication(), schedName));
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void resume() {
        try {
          scheduleClient.resume(Id.Schedule.from(programId.getApplication(), schedName));
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String status(int expectedCode) {
        try {
          return scheduleClient.getStatus(Id.Schedule.from(programId.getApplication(), schedName));
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}

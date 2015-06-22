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

package co.cask.cdap.test.internal;

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.AbstractProgramManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ScheduleManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.base.Throwables;

import java.util.List;

/**
 * A default implementation of {@link FlowManager}.
 */
public class DefaultWorkflowManager extends AbstractProgramManager<WorkflowManager> implements WorkflowManager {
  private final AppFabricClient appFabricClient;

  public DefaultWorkflowManager(Id.Program programId, AppFabricClient appFabricClient,
                                DefaultApplicationManager applicationManager) {
    super(programId, applicationManager);
    this.appFabricClient = appFabricClient;
  }

  @Override
  public List<ScheduleSpecification> getSchedules() {
    return appFabricClient.getSchedules(programId.getNamespaceId(), programId.getApplicationId(), programId.getId());
  }

  @Override
  public List<RunRecord> getHistory() {
    return appFabricClient.getHistory(programId.getNamespaceId(), programId.getApplicationId(), programId.getId());
  }

  public ScheduleManager getSchedule(final String schedName) {

    return new ScheduleManager() {
      @Override
      public void suspend() {
        try {
          appFabricClient.suspend(programId.getNamespaceId(), programId.getApplicationId(), schedName);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void resume() {
        try {
          appFabricClient.resume(programId.getNamespaceId(), programId.getApplicationId(), schedName);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String status(int expectedCode) {
        return appFabricClient.scheduleStatus(programId.getNamespaceId(), programId.getApplicationId(),
                                              schedName, expectedCode);
      }
    };
  }
}

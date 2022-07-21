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


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Trigger that schedules a ProgramSchedule, when a certain status of a program has been achieved.
 */
public class ProgramStatusTrigger extends ProtoTrigger.ProgramStatusTrigger implements SatisfiableTrigger {
  private static final Gson GSON = new Gson();

  public ProgramStatusTrigger(ProgramId programId, Set<ProgramStatus> programStatuses) {
    super(programId, programStatuses);
  }

  @VisibleForTesting
  public ProgramStatusTrigger(ProgramId programId, ProgramStatus... programStatuses) {
    super(programId, new HashSet<>(Arrays.asList(programStatuses)));
  }

  @Override
  public boolean isSatisfied(ProgramSchedule schedule, List<Notification> notifications) {
    return getTriggerSatisfiedResult(notifications, false, new Function<ProgramRunInfo, Boolean>() {
      @Override
      public Boolean apply(ProgramRunInfo input) {
        return true;
      }
    });
  }

  @Override
  public Set<String> getTriggerKeys() {
    return Schedulers.triggerKeysForProgramStatuses(programId, programStatuses);
  }

  @Override
  public List<TriggerInfo> getTriggerInfos(final TriggerInfoContext context) {
    Function<ProgramRunInfo, List<TriggerInfo>> function = new Function<ProgramRunInfo, List<TriggerInfo>>() {
      @Override
      public List<TriggerInfo> apply(ProgramRunInfo runInfo) {
        Map<String, String> runtimeArgs = context.getProgramRuntimeArguments(runInfo.getProgramRunId());
        TriggerInfo triggerInfo =
          new DefaultProgramStatusTriggerInfo(programId.getNamespace(),
                                              programId.getParent().getApplication(),
                                              ProgramType.valueOf(programId.getType().name()), programId.getProgram(),
                                              RunIds.fromString(runInfo.getProgramRunId().getRun()),
                                              runInfo.getProgramStatus(),
                                              context.getWorkflowToken(runInfo.getProgramRunId()), runtimeArgs);
        return Collections.singletonList(triggerInfo);
      }
    };
    return getTriggerSatisfiedResult(context.getNotifications(), ImmutableList.<TriggerInfo>of(), function);
  }

  @Override
  public void updateLaunchArguments(ProgramSchedule schedule, List<Notification> notifications,
                                    Map<String, String> systemArgs, Map<String, String> userArgs) {
    // no-op
  }

  /**
   * Helper method to return a result from the given supplier if the trigger is satisfied with the given notifications,
   * or return the default result if the trigger is not satisfied.
   *
   * @param notifications notifications used to determine whether the trigger is satisfied
   * @param defaultResult the default result to return if the trigger is not satisfied
   * @param function the function to get result from if the trigger is satisfied
   * @param <T> type of the result to be returned
   * @return a result of type T
   */
  private <T> T getTriggerSatisfiedResult(List<Notification> notifications, T defaultResult,
                                          Function<ProgramRunInfo, T> function) {
    for (Notification notification : notifications) {
      if (!Notification.Type.PROGRAM_STATUS.equals(notification.getNotificationType())) {
        continue;
      }
      String programRunIdString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programRunStatusString = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);
      // Ignore notifications which specify an invalid programRunId or programStatus
      if (programRunIdString == null || programRunStatusString == null) {
        continue;
      }
      ProgramStatus programStatus;
      try {
        programStatus = ProgramRunStatus.toProgramStatus(ProgramRunStatus.valueOf(programRunStatusString));
      } catch (IllegalArgumentException e) {
        // Return silently, this happens for statuses that are not meant to be scheduled
        continue;
      }
      ProgramRunId programRunId = GSON.fromJson(programRunIdString, ProgramRunId.class);
      ProgramId triggeringProgramId = programRunId.getParent();
      if (this.programId.equals(triggeringProgramId) && programStatuses.contains(programStatus)) {
        return function.apply(new ProgramRunInfo(programStatus, programRunId));
      }
    }
    return defaultResult;
  }

  /**
   * Class for storing program status and run id of a program run.
   */
  private static class ProgramRunInfo {
    private final ProgramStatus programStatus;
    private final ProgramRunId programRunId;

    ProgramRunInfo(ProgramStatus programStatus, ProgramRunId programRunId) {
      this.programStatus = programStatus;
      this.programRunId = programRunId;
    }

    ProgramStatus getProgramStatus() {
      return programStatus;
    }

    ProgramRunId getProgramRunId() {
      return programRunId;
    }
  }
}

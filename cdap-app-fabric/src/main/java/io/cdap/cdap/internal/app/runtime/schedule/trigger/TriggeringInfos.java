/*
 * Copyright © 2022 Cask Data, Inc.
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
import io.cdap.cdap.api.schedule.PartitionTriggerInfo;
import io.cdap.cdap.api.schedule.ProgramStatusTriggerInfo;
import io.cdap.cdap.api.schedule.TimeTriggerInfo;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.TriggeringInfo;
import io.cdap.cdap.proto.TriggeringPropertyMapping;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Util functions for {@link TriggeringInfo}
 */
public final class TriggeringInfos {

  private static final Gson gson = new Gson();
  public static final String TRIGGERING_PROPERTIES_MAPPING = "triggering.properties.mapping";

  private TriggeringInfos() {

  }

  /**
   * Create a new {@link TriggeringInfo} object given
   * {@link TriggeringScheduleInfo}, {@link ScheduleId} and {@link Trigger.Type}
   * @param triggeringScheduleInfo
   * @param type
   * @param scheduleId
   * @return
   */
  @Nullable
  public static TriggeringInfo fromTriggeringScheduleInfo(TriggeringScheduleInfo triggeringScheduleInfo,
                                                          Trigger.Type type, ScheduleId scheduleId) {
    List<TriggerInfo> triggerInfos = triggeringScheduleInfo.getTriggerInfos();
    switch(type) {
      case PROGRAM_STATUS:
        // always expected to have exactly one element
        ProgramStatusTriggerInfo programStatusTriggerInfo = (ProgramStatusTriggerInfo) triggerInfos.get(0);
        return new TriggeringInfo.ProgramStatusTriggeringInfo(scheduleId,
                                                              programStatusTriggerInfo.getRuntimeArguments(),
                                                              getProgramRunId(programStatusTriggerInfo));
      case TIME:
        // always expected to have exactly one element
        TimeTriggerInfo timeTriggerInfo = (TimeTriggerInfo) triggerInfos.get(0);
        return new TriggeringInfo.TimeTriggeringInfo(scheduleId, Collections.emptyMap(),
                                                     timeTriggerInfo.getCronExpression());
      case OR:
        return new TriggeringInfo.OrTriggeringInfo(getTriggeringInfoList(triggerInfos, scheduleId),
                                                   scheduleId, Collections.emptyMap(),
                                                   triggeringPropertyMapping(triggeringScheduleInfo.getProperties()));
      case AND:
        return new TriggeringInfo.AndTriggeringInfo(getTriggeringInfoList(triggerInfos, scheduleId),
                                                    scheduleId, Collections.emptyMap(),
                                                    triggeringPropertyMapping(triggeringScheduleInfo.getProperties()));
      case PARTITION:
        // always expected to have exactly one element
        PartitionTriggerInfo partitionTriggerInfo = (PartitionTriggerInfo) triggerInfos.get(0);
        return new TriggeringInfo.PartitionTriggeringInfo(scheduleId, Collections.emptyMap(),
                                                          partitionTriggerInfo.getDatasetName(),
                                                          partitionTriggerInfo.getDatasetNamespace(),
                                                          partitionTriggerInfo.getExpectedNumPartitions(),
                                                          partitionTriggerInfo.getActualNumPartitions());
      default:
        return null;
    }
  }

  private static ProgramRunId getProgramRunId(ProgramStatusTriggerInfo programStatusTriggerInfo) {
    return new ProgramRunId(programStatusTriggerInfo.getNamespace(), programStatusTriggerInfo.getApplicationName(),
                            ProgramType.valueOfApiProgramType(programStatusTriggerInfo.getProgramType()),
                            programStatusTriggerInfo.getProgram(), programStatusTriggerInfo.getRunId().getId());
  }

  @Nullable
  private static TriggeringPropertyMapping triggeringPropertyMapping(Map<String, String> properties) {
    if (!properties.containsKey(TRIGGERING_PROPERTIES_MAPPING)) {
      return null;
    }
    return gson.fromJson(properties.get(TRIGGERING_PROPERTIES_MAPPING), TriggeringPropertyMapping.class);
  }

  private static List<TriggeringInfo> getTriggeringInfoList(List<TriggerInfo> triggerInfos, ScheduleId scheduleId) {
    return triggerInfos.stream().map(r -> fromTriggerInfo(r, scheduleId)).collect(Collectors.toList());
  }

  @Nullable
  private static TriggeringInfo fromTriggerInfo(TriggerInfo triggerInfo, ScheduleId scheduleId) {
    switch(triggerInfo.getType()) {
      case PROGRAM_STATUS:
        ProgramStatusTriggerInfo progTrigger = (ProgramStatusTriggerInfo) triggerInfo;
        return new TriggeringInfo.ProgramStatusTriggeringInfo(
          scheduleId, progTrigger.getRuntimeArguments(), getProgramRunId(progTrigger));
      case TIME:
        TimeTriggerInfo timeTrigger = (TimeTriggerInfo) triggerInfo;
        return new TriggeringInfo.TimeTriggeringInfo(
          scheduleId, Collections.emptyMap(), timeTrigger.getCronExpression());
      case PARTITION:
        PartitionTriggerInfo partTrigger = (PartitionTriggerInfo) triggerInfo;
        return new TriggeringInfo.PartitionTriggeringInfo(
          scheduleId, Collections.emptyMap(), partTrigger.getDatasetName(), partTrigger.getDatasetNamespace(),
          partTrigger.getExpectedNumPartitions(), partTrigger.getActualNumPartitions());
      default:
        return null;
    }
  }
}

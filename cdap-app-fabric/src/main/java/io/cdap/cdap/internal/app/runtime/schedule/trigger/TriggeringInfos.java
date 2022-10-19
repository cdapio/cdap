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
import io.cdap.cdap.proto.ArgumentMapping;
import io.cdap.cdap.proto.PluginPropertyMapping;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunStartMetadata;
import io.cdap.cdap.proto.TriggeringInfo;
import io.cdap.cdap.proto.TriggeringPropertyMapping;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.spi.events.StartMetadata;
import io.cdap.cdap.spi.events.StartType;
import io.cdap.cdap.spi.events.trigger.CompositeTriggeringInfo;
import io.cdap.cdap.spi.events.trigger.PartitionTriggeringInfo;
import io.cdap.cdap.spi.events.trigger.ProgramStatusTriggeringInfo;
import io.cdap.cdap.spi.events.trigger.TimeTriggeringInfo;

import java.util.ArrayList;
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

  public static StartMetadata fromProtoToSpi(RunStartMetadata runStartMetadata) {
    return new StartMetadata(StartType.valueOfCategoryName(runStartMetadata.getType().getCategoryName()),
                             fromProtoToSpi(runStartMetadata.getTriggeringInfo()));
  }

  @Nullable
  private static io.cdap.cdap.spi.events.trigger.TriggeringInfo fromProtoToSpi(TriggeringInfo proto) {
    if (proto == null) {
      return null;
    }
    switch (proto.getType()) {
      case TIME:
        TriggeringInfo.TimeTriggeringInfo timeProto = (TriggeringInfo.TimeTriggeringInfo) proto;
        return new TimeTriggeringInfo(new io.cdap.cdap.spi.events.trigger
          .ScheduleId(timeProto.getScheduleId().getApplication(),
                      timeProto.getScheduleId().getSchedule()), timeProto.getCronExpression(),
                                      timeProto.getRuntimeArguments());
      case PROGRAM_STATUS:
        TriggeringInfo.ProgramStatusTriggeringInfo programProto = (TriggeringInfo.ProgramStatusTriggeringInfo) proto;
        return new ProgramStatusTriggeringInfo(new io.cdap.cdap.spi.events.trigger
          .ScheduleId(programProto.getScheduleId().getApplication(),
                      programProto.getScheduleId().getSchedule()),
                                               programProto.getProgramRunId().getRun(),
                                               programProto.getProgramRunId().getProgram(),
                                               programProto.getProgramRunId().getApplication(),
                                               programProto.getProgramRunId().getNamespace(),
                                               programProto.getRuntimeArguments());
      case PARTITION:
        TriggeringInfo.PartitionTriggeringInfo partitionProto = (TriggeringInfo.PartitionTriggeringInfo) proto;
        return new PartitionTriggeringInfo(partitionProto.getDatasetNamespace(),
                                           partitionProto.getDatasetName(),
                                           partitionProto.getExpectedNumPartitions(),
                                           partitionProto.getActualNumPartitions());
      case AND:
        TriggeringInfo.AndTriggeringInfo andProto = (TriggeringInfo.AndTriggeringInfo) proto;
        return new CompositeTriggeringInfo(io.cdap.cdap.spi.events.trigger.TriggeringInfo.Type.AND,
                                           new io.cdap.cdap.spi.events.trigger
                                             .ScheduleId(andProto.getScheduleId().getApplication(),
                                                         andProto.getScheduleId().getSchedule()),
                                           andProto.getTriggeringInfos().stream()
                                             .map(TriggeringInfos::fromProtoToSpi).collect(Collectors.toList()),
                                           fromProtoToSpi(andProto.getTriggeringPropertyMapping()));
      case OR:
        TriggeringInfo.OrTriggeringInfo orProto = (TriggeringInfo.OrTriggeringInfo) proto;
        return new CompositeTriggeringInfo(io.cdap.cdap.spi.events.trigger.TriggeringInfo.Type.AND,
                                           new io.cdap.cdap.spi.events.trigger
                                             .ScheduleId(orProto.getScheduleId().getApplication(),
                                                         orProto.getScheduleId().getSchedule()),
                                           orProto.getTriggeringInfos().stream()
                                             .map(TriggeringInfos::fromProtoToSpi).collect(Collectors.toList()),
                                           fromProtoToSpi(orProto.getTriggeringPropertyMapping()));
      default:
        return null;
    }
  }

  @Nullable
  private static io.cdap.cdap.spi.events.trigger
    .TriggeringPropertyMapping fromProtoToSpi(TriggeringPropertyMapping proto) {
    if (proto == null) {
      return null;
    }
    List<io.cdap.cdap.spi.events.trigger.ArgumentMapping> argumentMappings = new ArrayList<>();
    List<io.cdap.cdap.spi.events.trigger.PluginPropertyMapping> pluginMappings = new ArrayList<>();
    if (proto.getArguments() != null) {
      for (ArgumentMapping arg : proto.getArguments()) {
        argumentMappings.add(new io.cdap.cdap.spi.events.trigger
          .ArgumentMapping(arg.getSource(), arg.getTarget(), arg.getTriggeringPipelineId().getNamespace(),
                           arg.getTriggeringPipelineId().getPipelineName()));
      }
    }
    if (proto.getPluginProperties() != null) {
      for (PluginPropertyMapping plug : proto.getPluginProperties()) {
        pluginMappings.add(new io.cdap.cdap.spi.events.trigger
          .PluginPropertyMapping(plug.getStageName(), plug.getSource(), plug.getTarget(),
                                 plug.getTriggeringPipelineId().getNamespace(),
                                 plug.getTriggeringPipelineId().getPipelineName()));
      }
    }
    return new io.cdap.cdap.spi.events.trigger.TriggeringPropertyMapping(argumentMappings, pluginMappings);
  }
}

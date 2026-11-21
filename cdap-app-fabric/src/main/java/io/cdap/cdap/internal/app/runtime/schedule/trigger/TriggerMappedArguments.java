/*
 * Copyright © 2026 Cask Data, Inc.
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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.partitioned.PartitionKeyCodec;
import io.cdap.cdap.api.schedule.ProgramStatusTriggerInfo;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.workflow.Value;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts runtime argument and plugin-property mappings from the {@code triggeringScheduleInfo}
 * system argument without throwing, mirroring {@code SmartWorkflow} and {@code PipelineTriggers}.
 */
public final class TriggerMappedArguments {

  private static final Logger LOG = LoggerFactory.getLogger(TriggerMappedArguments.class);
  private static final Gson GSON = new Gson();
  // Match ScheduleTaskRunner (writer) and AbstractContext (runtime reader).
  private static final Gson SCHEDULE_GSON = TriggeringScheduleInfoAdapter
      .addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(PartitionKey.class, new PartitionKeyCodec())
      .create();
  private static final Type STAGE_PROPERTIES_MAP =
      new TypeToken<Map<String, Map<String, String>>>() {
      }.getType();

  // Keep in sync with SmartWorkflow.TRIGGERING_PROPERTIES_MAPPING and
  // PipelineTriggers.RESOLVED_PLUGIN_PROPERTIES_MAP in cdap-data-pipeline-base.
  static final String TRIGGERING_PROPERTIES_MAPPING = "triggering.properties.mapping";
  static final String RESOLVED_PLUGIN_PROPERTIES_MAP = "resolved.plugin.properties.map";

  private TriggerMappedArguments() {
  }

  /**
   * Outcome of reading trigger-mapped arguments from system arguments.
   */
  public static final class Result {

    private static final Result EMPTY = new Result(Collections.emptyMap(), false);
    private static final Result UNREADABLE = new Result(Collections.emptyMap(), true);

    private final Map<String, String> arguments;
    private final boolean unreadable;

    private Result(Map<String, String> arguments, boolean unreadable) {
      this.arguments = arguments;
      this.unreadable = unreadable;
    }

    public static Result empty() {
      return EMPTY;
    }

    /**
     * Returns a readable {@link Result} holding a defensive copy of {@code arguments}, or
     * {@link #empty()} if {@code arguments} is {@code null} or empty.
     */
    public static Result of(@Nullable Map<String, String> arguments) {
      if (arguments == null || arguments.isEmpty()) {
        return EMPTY;
      }
      return new Result(Collections.unmodifiableMap(new HashMap<>(arguments)), false);
    }

    public static Result unreadable() {
      return UNREADABLE;
    }

    public Map<String, String> getArguments() {
      return arguments;
    }

    public boolean isUnreadable() {
      return unreadable;
    }
  }

  /**
   * Extracts trigger-mapped arguments from {@code systemArgs}. Returns {@link Result#empty()} when
   * no triggering schedule mapping applies, or {@link Result#unreadable()} if the schedule info or
   * mapping cannot be read cleanly. Never throws.
   *
   * @param systemArgs the run's system arguments
   * @param compositeTriggerEnabled whether composite pipeline triggers are enabled
   * @return the extracted mapped arguments or an unreadable indicator
   */
  public static Result extract(@Nullable Map<String, String> systemArgs,
      boolean compositeTriggerEnabled) {
    if (systemArgs == null
        || !systemArgs.containsKey(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO)) {
      return Result.empty();
    }
    String rawScheduleInfo = systemArgs.get(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO);
    if (Strings.isNullOrEmpty(rawScheduleInfo)) {
      return Result.empty();
    }
    try {
      TriggeringScheduleInfo scheduleInfo =
          SCHEDULE_GSON.fromJson(rawScheduleInfo, TriggeringScheduleInfo.class);
      if (scheduleInfo == null) {
        LOG.warn("Deserialized null triggeringScheduleInfo; treating trigger mapping as unreadable");
        return Result.unreadable();
      }
      Map<String, String> scheduleProperties = scheduleInfo.getProperties();
      if (scheduleProperties == null) {
        return Result.empty();
      }
      String propertiesMappingJson = scheduleProperties.get(TRIGGERING_PROPERTIES_MAPPING);
      if (propertiesMappingJson == null) {
        return Result.empty();
      }
      PropertyMappingSpec mappingSpec = GSON.fromJson(
          propertiesMappingJson, PropertyMappingSpec.class);
      if (mappingSpec == null) {
        LOG.warn("Deserialized null {}; treating trigger mapping as unreadable",
            TRIGGERING_PROPERTIES_MAPPING);
        return Result.unreadable();
      }

      List<ProgramStatusTriggerInfo> statusTriggers = collectProgramStatusTriggers(scheduleInfo);
      if (statusTriggers.isEmpty()) {
        return Result.empty();
      }

      List<ProgramStatusTriggerInfo> triggersToApply = compositeTriggerEnabled
          ? statusTriggers : Collections.singletonList(statusTriggers.get(0));
      Map<String, String> mappings = new HashMap<>();
      for (ProgramStatusTriggerInfo triggerInfo : triggersToApply) {
        if (!applyTriggerMappings(mappings, triggerInfo, mappingSpec, compositeTriggerEnabled)) {
          return Result.unreadable();
        }
      }
      return Result.of(mappings);
    } catch (Exception | StackOverflowError e) {
      LOG.warn("Failed to read triggering schedule property mappings", e);
      return Result.unreadable();
    }
  }

  private static List<ProgramStatusTriggerInfo> collectProgramStatusTriggers(
      TriggeringScheduleInfo scheduleInfo) {
    List<TriggerInfo> triggerInfos = scheduleInfo.getTriggerInfos();
    if (triggerInfos == null || triggerInfos.isEmpty()) {
      return Collections.emptyList();
    }
    List<ProgramStatusTriggerInfo> result = new ArrayList<>();
    for (TriggerInfo info : triggerInfos) {
      if (info instanceof ProgramStatusTriggerInfo) {
        result.add((ProgramStatusTriggerInfo) info);
      }
    }
    return result;
  }

  /**
   * Applies argument and plugin-property mappings for a single {@link ProgramStatusTriggerInfo}.
   * Returns {@code false} when the mapping data is malformed or required resolved plugin
   * properties are unavailable.
   */
  private static boolean applyTriggerMappings(Map<String, String> mappings,
      ProgramStatusTriggerInfo triggerInfo, PropertyMappingSpec spec,
      boolean compositeTriggerEnabled) {
    if (triggerInfo.getNamespace() == null || triggerInfo.getApplicationName() == null) {
      LOG.warn("ProgramStatusTriggerInfo is missing namespace or application name; "
          + "treating trigger mapping as unreadable");
      return false;
    }
    Map<String, String> triggeringArguments = buildTriggeringArguments(triggerInfo);

    if (spec.arguments != null) {
      for (ArgumentMappingSpec mapping : spec.arguments) {
        if (mapping == null) {
          LOG.warn("Null entry in argument mappings; treating trigger mapping as unreadable");
          return false;
        }
        if (compositeTriggerEnabled && mapping.pipeline != null
            && !matchesPipeline(mapping.pipeline, triggerInfo)) {
          continue;
        }
        if (mapping.source == null) {
          continue;
        }
        String value = triggeringArguments.get(mapping.source);
        if (value == null) {
          continue;
        }
        String targetKey = mapping.target == null ? mapping.source : mapping.target;
        mappings.put(targetKey, value);
      }
    }

    if (spec.pluginProperties != null && !spec.pluginProperties.isEmpty()) {
      Map<String, Map<String, String>> resolvedProperties = null;
      for (PluginPropertyMappingSpec mapping : spec.pluginProperties) {
        if (mapping == null) {
          LOG.warn("Null entry in plugin property mappings; treating trigger mapping as unreadable");
          return false;
        }
        if (compositeTriggerEnabled && mapping.pipeline != null
            && !matchesPipeline(mapping.pipeline, triggerInfo)) {
          continue;
        }
        if (mapping.stageName == null) {
          continue;
        }
        if (resolvedProperties == null) {
          String rawResolved = triggeringArguments.get(RESOLVED_PLUGIN_PROPERTIES_MAP);
          if (rawResolved != null) {
            resolvedProperties = GSON.fromJson(rawResolved, STAGE_PROPERTIES_MAP);
          }
        }
        if (resolvedProperties == null) {
          LOG.warn("Resolved plugin properties unavailable for stage '{}' from triggering "
              + "pipeline '{}' in namespace '{}'; treating trigger mapping as unreadable",
              mapping.stageName, triggerInfo.getApplicationName(), triggerInfo.getNamespace());
          return false;
        }
        Map<String, String> stageProps = resolvedProperties.get(mapping.stageName);
        if (stageProps == null || mapping.source == null) {
          continue;
        }
        String value = stageProps.get(mapping.source);
        if (value == null) {
          continue;
        }
        String targetKey = mapping.target == null ? mapping.source : mapping.target;
        mappings.put(targetKey, value);
      }
    }
    return true;
  }

  private static Map<String, String> buildTriggeringArguments(
      ProgramStatusTriggerInfo triggerInfo) {
    Map<String, String> args = new HashMap<>();
    if (triggerInfo.getRuntimeArguments() != null) {
      args.putAll(triggerInfo.getRuntimeArguments());
    }
    WorkflowToken token = triggerInfo.getWorkflowToken();
    if (token != null) {
      for (String tokenKey : token.getAll(WorkflowToken.Scope.USER).keySet()) {
        Value value = token.get(tokenKey, WorkflowToken.Scope.USER);
        if (value != null) {
          args.put(tokenKey, value.toString());
        }
      }
    }
    return args;
  }

  private static boolean matchesPipeline(PipelineIdSpec pipeline,
      ProgramStatusTriggerInfo triggerInfo) {
    return pipeline.namespace != null
        && pipeline.name != null
        && pipeline.namespace.equals(triggerInfo.getNamespace())
        && pipeline.name.equals(triggerInfo.getApplicationName());
  }

  // Keep field names in sync with io.cdap.cdap.etl.proto.v2.TriggeringPropertyMapping,
  // ArgumentMapping, PluginPropertyMapping, and TriggeringPipelineId.
  private static final class PropertyMappingSpec {
    @Nullable
    private List<ArgumentMappingSpec> arguments;
    @Nullable
    private List<PluginPropertyMappingSpec> pluginProperties;
  }

  private static class ArgumentMappingSpec {
    @Nullable
    String source;
    @Nullable
    String target;
    @Nullable
    PipelineIdSpec pipeline;
  }

  private static final class PluginPropertyMappingSpec extends ArgumentMappingSpec {
    @Nullable
    private String stageName;
  }

  private static final class PipelineIdSpec {
    @Nullable
    private String namespace;
    @Nullable
    private String name;
  }
}

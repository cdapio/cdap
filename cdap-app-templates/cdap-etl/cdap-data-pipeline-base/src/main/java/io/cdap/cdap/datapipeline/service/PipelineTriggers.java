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


package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.schedule.ProgramStatusTriggerInfo;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.FieldOperationTypeAdapter;
import io.cdap.cdap.etl.proto.v2.ArgumentMapping;
import io.cdap.cdap.etl.proto.v2.PluginPropertyMapping;
import io.cdap.cdap.etl.proto.v2.TriggeringPipelineId;
import io.cdap.cdap.etl.proto.v2.TriggeringPropertyMapping;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Handles parsing of pipeline runtime arguments mapping.
 */
public final class PipelineTriggers {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(FieldOperation.class, new FieldOperationTypeAdapter()).create();
  private static final String RESOLVED_PLUGIN_PROPERTIES_MAP = "resolved.plugin.properties.map";
  private static final Type STAGE_PROPERTIES_MAP = new TypeToken<Map<String, Map<String, String>>>() { }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(PipelineTriggers.class);

  private PipelineTriggers() {
  }

  /**
   * Parse the arguments mapping from the pipeline schedule and add to mappings.
   * If compositeTriggerEnabled is true, the code will iterate through all the argument mappings and only adds
   * those which match pipeline IDs.
   * Example: for given TriggeringPropertyMapping as "{"arguments":[{"source":"source1","target":"target1","pipelineId"
   * :{"namespace":"default","pipelineName":"Pipeline1"}}，{"source":"source2","target":"target2","pipelineId":
   * {"namespace":"default","pipelineName":"Pipeline2"}}]}" and a trigger {"namespace":"default","pipelineName":
   * "Pipeline1"} which has runtime arguments "{"source1": "value1","source2":"value2"}". When composite pipeline
   * trigger is enabled, only {"target1": "value1"} is added. Otherwise, the code will ignore the ID check and
   * {"target1": "value1", "target2": "value2"} is added.
   *
   * @param triggerInfo {@link ProgramStatusTriggerInfo} with plugin properties
   * @param propertiesMapping  {@link TriggeringPropertyMapping} for using the plugin
   * @param compositeTriggerEnabled  whether the composite trigger is enabled
   */
  public static void addSchedulePropertiesMapping(Map<String, String> mappings,
                                                  ProgramStatusTriggerInfo triggerInfo,
                                                  TriggeringPropertyMapping propertiesMapping,
                                                  boolean compositeTriggerEnabled) {
    TriggeringPipelineId pipelineId = new TriggeringPipelineId(
      triggerInfo.getNamespace(),
      triggerInfo.getApplicationName());

    BasicArguments triggeringArguments = new BasicArguments(
      triggerInfo.getWorkflowToken(),
      triggerInfo.getRuntimeArguments());

    // Get the value of every triggering pipeline arguments specified in the propertiesMapping and update newRuntimeArgs
    List<ArgumentMapping> argumentMappings = propertiesMapping.getArguments();
    for (ArgumentMapping mapping : argumentMappings) {
      if (compositeTriggerEnabled
          && mapping.getTriggeringPipelineId() != null // Skip checking pipelineId for pre 6.8 triggers
          && !pipelineId.equals(mapping.getTriggeringPipelineId())) {
        continue;
      }
      String sourceKey = mapping.getSource();
      if (sourceKey == null) {
        LOG.warn("The name of argument from the triggering pipeline cannot be null, " +
          "skip this argument mapping: '{}'.", mapping);
        continue;
      }
      String value = triggeringArguments.get(sourceKey);
      if (value == null) {
        LOG.warn("Runtime argument '{}' is not found in run '{}' of the triggering "
            + "pipeline '{}' in namespace '{}' ",
            sourceKey, triggerInfo.getRunId(), triggerInfo.getApplicationName(),
          triggerInfo.getNamespace());
        continue;
      }
      // Use the argument name in the triggering pipeline if target is not specified
      String targetKey = mapping.getTarget() == null ? sourceKey : mapping.getTarget();
      mappings.put(targetKey, value);
    }
    // Get the resolved plugin properties map from triggering pipeline's workflow token in triggeringArguments
    Map<String, Map<String, String>> resolvedProperties =
      GSON.fromJson(triggeringArguments.get(RESOLVED_PLUGIN_PROPERTIES_MAP), STAGE_PROPERTIES_MAP);
    for (PluginPropertyMapping mapping : propertiesMapping.getPluginProperties()) {
      if (compositeTriggerEnabled
          && mapping.getTriggeringPipelineId() != null // Skip checking pipelineId for pre 6.8 triggers
          && !pipelineId.equals(mapping.getTriggeringPipelineId())) {
        continue;
      }
      String stageName = mapping.getStageName();
      if (stageName == null) {
        LOG.warn("The name of the stage cannot be null in plugin property mapping, "
          + "skip this mapping: '{}'.", mapping);
        continue;
      }
      Map<String, String> pluginProperties = resolvedProperties.get(stageName);
      if (pluginProperties == null) {
        LOG.warn("No plugin properties can be found with stage name '{}' in triggering "
            + "pipeline '{}' in namespace '{}' ", mapping.getStageName(), triggerInfo.getApplicationName(),
          triggerInfo.getNamespace());
        continue;
      }
      String sourceKey = mapping.getSource();
      if (sourceKey == null) {
        LOG.warn("The name of argument from the triggering pipeline cannot be null, " +
            "skip this argument mapping: '{}'.", mapping);
        continue;
      }
      String value = pluginProperties.get(sourceKey);
      if (value == null) {
        LOG.warn("No property with name '{}' can be found in plugin "
            + "'{}' of the triggering pipeline '{}' "
            + "in namespace '{}' ", sourceKey, stageName, triggerInfo.getApplicationName(),
          triggerInfo.getNamespace());
        continue;
      }
      // Use the argument name in the triggering pipeline if target is not specified
      String targetKey = mapping.getTarget() == null ? sourceKey : mapping.getTarget();
      mappings.put(targetKey, value);
    }
  }
}

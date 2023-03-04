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

package io.cdap.cdap.etl.spec;

import io.cdap.cdap.etl.api.engine.sql.BatchSQLEngine;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLTransformationPushdown;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to help extract pipeline properties from the program runtime arguments.
 */

public final class PipelineArguments {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineArguments.class);
  // Keys for pipeline configures
  public static final String INSTRUMENTATION_KEY = "app.pipeline.instrumentation";
  public static final String SPARK_CUSTOM_CONFIG_PREFIX = "system.spark.";
  public static final String PUSHDOWN_ENABLED_KEY = "app.pipeline.pushdownEnabled";
  public static final String TRANSFORMATION_PUSHDOWN_PREFIX = "app.pipeline.pushdown.plugin.";
  public static final String TRANSFORMATION_PUSHDOWN_NAME = TRANSFORMATION_PUSHDOWN_PREFIX + "name";
  public static final String TRANSFORMATION_PUSHDOWN_LABEL =
      TRANSFORMATION_PUSHDOWN_PREFIX + "label";
  public static final String TRANSFORMATION_PUSHDOWN_ARTIFACT_PREFIX =
      TRANSFORMATION_PUSHDOWN_PREFIX + "artifact.";
  public static final String TRANSFORMATION_PUSHDOWN_PROPERTIES_PREFIX =
      TRANSFORMATION_PUSHDOWN_PREFIX + "properties.";
  public static final String PIPELINE_CONFIG_OVERWRITE = "app.pipeline.overwriteConfig";

  /**
   * @param args the arguments to lookup spark engine custom config settings
   * @param originalProperties default value to use if spark engine custom config settings are
   *     missing from the arguments
   */
  public static Map<String, String> getEngineProperties(Map<String, String> args,
      Map<String, String> originalProperties) {
    if (!args.containsKey(PIPELINE_CONFIG_OVERWRITE)) {
      return originalProperties;
    }
    Map<String, String> properties = new HashMap<String, String>();
    for (String name : args.keySet()) {
      if (!name.startsWith(SPARK_CUSTOM_CONFIG_PREFIX)) {
        continue;
      }
      String value = args.get(name);
      String key = name.substring(SPARK_CUSTOM_CONFIG_PREFIX.length());
      if (key != null && value != null) {
        properties.put(key, value);
      }
    }
    LOG.info("Overwrite orignal engine config {} with {}", originalProperties, properties);
    return Collections.unmodifiableMap(properties);
  }

  /**
   * @param args the arguments to lookup transformation pushdown settings
   * @param originalTransformationPushdown default value to use if transformation pushdown is
   *     missing from the arguments
   */
  public static ETLTransformationPushdown getTransformationPushdown(
      Map<String, String> args, @Nullable ETLTransformationPushdown originalTransformationPushdown
  ) {
    if (!args.containsKey(PIPELINE_CONFIG_OVERWRITE)) {
      return originalTransformationPushdown;
    }
    String name = "";
    // type should not be overwritable
    String type = BatchSQLEngine.PLUGIN_TYPE;
    Map<String, String> propertiesMap = new HashMap<>();
    Map<String, String> artifactMap = new HashMap<>();
    String label = "";
    for (String key : args.keySet()) {
      if (!key.startsWith(TRANSFORMATION_PUSHDOWN_PREFIX)) {
        continue;
      }
      if (key.equals(TRANSFORMATION_PUSHDOWN_NAME)) {
        name = args.get(key);
      } else if (key.equals(TRANSFORMATION_PUSHDOWN_LABEL)) {
        label = args.get(key);
      } else if (key.startsWith(TRANSFORMATION_PUSHDOWN_ARTIFACT_PREFIX)) {
        String artifactKey = key.substring(TRANSFORMATION_PUSHDOWN_ARTIFACT_PREFIX.length());
        artifactMap.put(artifactKey, args.get(key));
      } else if (key.startsWith(TRANSFORMATION_PUSHDOWN_PROPERTIES_PREFIX)) {
        String propertyKey = key.substring(TRANSFORMATION_PUSHDOWN_PROPERTIES_PREFIX.length());
        propertiesMap.put(propertyKey, args.get(key));
      }
    }
    try {
      ArtifactSelectorConfig artifact = new ArtifactSelectorConfig(artifactMap.get("scope"),
          artifactMap.get("name"),
          artifactMap.get("version"));
      ETLTransformationPushdown newTransformationPushdown = new ETLTransformationPushdown(
          new ETLPlugin(name, type, propertiesMap, artifact, label)
      );
      LOG.info("Overwrite orginal pushdown {} with {}", originalTransformationPushdown,
          newTransformationPushdown);
      return newTransformationPushdown;
    } catch (Exception e) {
      LOG.info("Cannot parse pushdown runtime arguments, use pushdown config from app spec now", e);
    }
    return originalTransformationPushdown;
  }

  /**
   * @param args the arguments to lookup transformation pushdown enabled settings
   * @param originalPushdownEnabled default value to use if transformation pushdown enabled
   *     settings are missing from the arguments
   */
  public static boolean isPushdownEnabled(Map<String, String> args,
      boolean originalPushdownEnabled) {
    return !args.containsKey(PUSHDOWN_ENABLED_KEY) || !args.containsKey(PIPELINE_CONFIG_OVERWRITE)
        ? originalPushdownEnabled
        : Boolean.parseBoolean(args.get(PUSHDOWN_ENABLED_KEY));
  }

  /**
   * @param args the arguments to lookup instrumentation settings
   * @param originalProcessTimingEnabled default value to use if instrumentation settings are
   *     missing from the arguments
   */
  public static boolean isProcessTimingEnabled(Map<String, String> args,
      boolean originalProcessTimingEnabled) {
    return !args.containsKey(INSTRUMENTATION_KEY) || !args.containsKey(PIPELINE_CONFIG_OVERWRITE)
        ? originalProcessTimingEnabled
        : Boolean.parseBoolean(args.get(INSTRUMENTATION_KEY));
  }
}

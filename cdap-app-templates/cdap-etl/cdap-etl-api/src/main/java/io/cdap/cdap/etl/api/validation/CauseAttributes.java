/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.validation;

import io.cdap.cdap.api.annotation.Beta;

/**
 * Cause attributes constants.
 */
@Beta
public class CauseAttributes {

  // Represents stage configuration property failure
  public static final String STAGE_CONFIG = "stageConfig";
  // Represents an element in the list of elements associated with a stage config property.
  // For example, in projection transform, config property 'keep' represents a list of input fields to keep. Below
  // cause attribute can be used to represent an invalid field in 'keep' config property
  public static final String CONFIG_ELEMENT = "configElement";

  // Represents id of the plugin
  public static final String PLUGIN_ID = "pluginId";
  // Represents type of the plugin
  public static final String PLUGIN_TYPE = "pluginType";
  // Represents name of the plugin
  public static final String PLUGIN_NAME = "pluginName";
  // Represents requested artifact name
  public static final String REQUESTED_ARTIFACT_NAME = "requestedArtifactName";
  // Represents requested artifact name
  public static final String REQUESTED_ARTIFACT_VERSION = "requestedArtifactVersion";
  // Represents requested artifact scope
  public static final String REQUESTED_ARTIFACT_SCOPE = "requestedArtifactScope";
  // Represents suggested artifact name
  public static final String SUGGESTED_ARTIFACT_NAME = "suggestedArtifactName";
  // Represents suggested artifact name
  public static final String SUGGESTED_ARTIFACT_VERSION = "suggestedArtifactVersion";
  // Represents suggested artifact scope
  public static final String SUGGESTED_ARTIFACT_SCOPE = "suggestedArtifactScope";

  // Represents input stage
  public static final String INPUT_STAGE = "inputStage";
  // Represents field of input stage schema
  public static final String INPUT_SCHEMA_FIELD = "inputField";

  // Represents a stage output port
  public static final String OUTPUT_PORT = "outputPort";
  // Represents a field of output stage schema
  public static final String OUTPUT_SCHEMA_FIELD = "outputField";

  // Represents a stacktrace
  public static final String STACKTRACE = "stacktrace";
}

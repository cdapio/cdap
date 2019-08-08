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

  // represents stage configuration property failure
  public static final String STAGE_CONFIG = "stageConfig";

  // represents id of the plugin
  public static final String PLUGIN_ID = "pluginId";
  // represents type of the plugin
  public static final String PLUGIN_TYPE = "pluginType";
  // represents name of the plugin
  public static final String PLUGIN_NAME = "pluginName";

  // represents input stage
  public static final String INPUT_STAGE = "inputStage";
  // represents field of input stage schema
  public static final String INPUT_SCHEMA_FIELD = "inputField";

  // represents a stage output port
  public static final String OUTPUT_PORT = "outputPort";
  // represents a field of output stage schema
  public static final String OUTPUT_SCHEMA_FIELD = "outputField";
}

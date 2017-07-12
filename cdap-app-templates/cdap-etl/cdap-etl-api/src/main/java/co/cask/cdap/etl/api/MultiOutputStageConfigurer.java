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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.schema.Schema;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * This stores the input schemas that are passed to this stage from other stages in the pipeline and
 * the output schema that could be sent to the next stages from this stage.
 */
@Beta
public interface MultiOutputStageConfigurer {

  /**
   * Get the input schema for this stage, or null if its unknown
   *
   * @return input schema
   */
  @Nullable
  Schema getInputSchema();

  /**
   * Set schema for each output port. If the schema for an output port is not known, the port should be placed in
   * the map with a null value.
   *
   * @param outputSchemas map of output port to its schema
   */
  void setOutputSchemas(Map<String, Schema> outputSchemas);
}

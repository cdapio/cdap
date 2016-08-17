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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.schema.Schema;

import javax.annotation.Nullable;

/**
 * This stores the input schema that is passed to this stage from other stages in the pipeline and
 * the output schema that could be sent to the next stages from this stage.
 */
@Beta
public interface StageConfigurer {

  /**
   * get the input schema for this stage, or null if its unknown
   *
   * @return input schema
   */
  @Nullable
  Schema getInputSchema();

  /**
   * set output schema for this stage, or null if its unknown
   *
   * @param outputSchema output schema for this stage
   */
  void setOutputSchema(@Nullable Schema outputSchema);

}

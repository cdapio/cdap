/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.api;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This stores the input schemas that are passed to this stage from other stages in the pipeline and
 * the output schema that could be sent to the next stages from this stage.
 */
@Beta
public interface MultiInputStageConfigurer {
  /**
   * get the map of input stageName to input schema for this stage, or return empty map if its unknown
   *
   * @return map of input schemas
   */
  Map<String, Schema> getInputSchemas();

  /**
   * get a list of input stage names.
   *
   * @return list of input stage names
   */
  List<String> getInputStages();

  /**
   * set output schema for this stage, or null if its unknown
   *
   * @param outputSchema output schema for this stage
   */
  void setOutputSchema(@Nullable Schema outputSchema);

  /**
   * Returns a failure collector for the stage.
   *
   * @return a failure collector
   * @throws UnsupportedOperationException if the implementation does not override this method
   */
  default FailureCollector getFailureCollector() {
    throw new UnsupportedOperationException("Getting failure collector is not supported.");
  }
}

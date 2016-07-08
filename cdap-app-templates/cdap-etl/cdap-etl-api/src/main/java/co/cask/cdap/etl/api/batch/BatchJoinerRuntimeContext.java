/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.api.batch;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Runtime context for batch joiner
 */
public interface BatchJoinerRuntimeContext extends BatchRuntimeContext {

  /**
   * Returns a map of input schemas for joiner. If the input schema for an input is null, the map will have input
   * name with null schema for that input
   * @return a map of input stage name to input schema for all the inputs to joiner
   */
  Map<String, Schema> getInputSchemas();

  /**
   * Returns output schema configured in {@link BatchJoiner#configurePipeline(PipelineConfigurer)} at configure time.
   * If not configured it will return null
   * @return output schema {@link Schema} for joiner
   */
  @Nullable
  Schema getOutputSchema();
}

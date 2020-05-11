/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.batch;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.etl.api.MultiInputPipelineConfigurable;
import io.cdap.cdap.etl.api.MultiInputPipelineConfigurer;
import io.cdap.cdap.etl.api.join.AutoJoiner;

/**
 * Joins input data, leaving implementation details up to the application.
 */
@Beta
public abstract class BatchAutoJoiner extends MultiInputBatchConfigurable<BatchJoinerContext>
  implements AutoJoiner, MultiInputPipelineConfigurable {
  public static final String PLUGIN_TYPE = BatchJoiner.PLUGIN_TYPE;

  /**
   * Configure the pipeline. This is run once when the pipeline is being published.
   * This is where you perform any static logic, like creating required datasets, performing schema validation,
   * setting output schema, and things of that nature.
   *
   * @param multiInputPipelineConfigurer the configurer used to add required datasets and streams
   */
  @Override
  public void configurePipeline(MultiInputPipelineConfigurer multiInputPipelineConfigurer) {
    // no-op
  }

  /**
   * Prepare a pipeline run. This is run every time before a pipeline runs in order to help set up the run.
   * This is where you would set things like the number of partitions to use when joining, and setting the
   * join key class if they are not known at compile time.
   *
   * @param context batch execution context
   * @throws Exception
   */
  @Override
  public void prepareRun(BatchJoinerContext context) throws Exception {
    //no-op
  }
}

/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.engine.sql;

import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchContext;

/**
 * Base implementation for the SQLEngine interface.
 * @param <KEY_IN>
 * @param <VALUE_IN>
 * @param <KEY_OUT>
 * @param <VALUE_OUT>
 */
public abstract class BatchSQLEngine<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT>
  implements SQLEngine<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {
  public static final String PLUGIN_TYPE = "sqlengine";

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  @Override
  public void prepareRun(BatchContext context) throws Exception {
    // no-op
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchContext context) {
    // no-op
  }
}

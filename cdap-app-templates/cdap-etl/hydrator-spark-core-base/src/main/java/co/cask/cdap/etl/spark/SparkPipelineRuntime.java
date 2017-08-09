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

package co.cask.cdap.etl.spark;

import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.PipelineRuntime;

/**
 * PipelineRuntime that can be created using Spark contexts.
 */
public class SparkPipelineRuntime extends PipelineRuntime {

  public SparkPipelineRuntime(SparkClientContext context) {
    super(context.getNamespace(), context.getApplicationSpecification().getName(), context.getLogicalStartTime(),
          new BasicArguments(context), context.getMetrics(), context, context);
  }

  public SparkPipelineRuntime(JavaSparkExecutionContext sec) {
    this(sec, sec.getLogicalStartTime());
  }

  public SparkPipelineRuntime(JavaSparkExecutionContext sec, long batchTime) {
    super(sec.getNamespace(), sec.getApplicationSpecification().getName(), batchTime,
          new BasicArguments(sec), sec.getMetrics(), sec.getPluginContext(), sec.getServiceDiscoverer());
  }
}

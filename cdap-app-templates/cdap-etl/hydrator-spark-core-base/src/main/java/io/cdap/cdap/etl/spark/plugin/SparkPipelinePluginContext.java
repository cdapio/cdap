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

package io.cdap.cdap.etl.spark.plugin;

import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.spark.JavaSparkMain;
import io.cdap.cdap.api.spark.SparkMain;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.batch.SparkSource;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.plugin.Caller;
import io.cdap.cdap.etl.common.plugin.PipelinePluginContext;

/**
 * Wraps spark specific plugin types.
 */
public class SparkPipelinePluginContext extends PipelinePluginContext {

  public SparkPipelinePluginContext(PluginContext delegate, Metrics metrics,
                                    boolean stageLoggingEnabled, boolean processTimingEnabled) {
    super(delegate, metrics, stageLoggingEnabled, processTimingEnabled);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Object wrapUnknownPlugin(String pluginId, Object plugin, Caller caller) {
    if (plugin instanceof Windower) {
      return new WrappedWindower((Windower) plugin, caller);
    } else if (plugin instanceof SparkSource) {
      return new WrappedSparkSource<>((SparkSource) plugin, caller);
    } else if (plugin instanceof SparkCompute) {
      return new WrappedSparkCompute<>((SparkCompute) plugin, caller);
    } else if (plugin instanceof SparkSink) {
      return new WrappedSparkSink<>((SparkSink) plugin, caller);
    } else if (plugin instanceof StreamingSource) {
      return new WrappedStreamingSource<>((StreamingSource) plugin, caller);
    } else if (plugin instanceof JavaSparkMain) {
      return new WrappedJavaSparkMain((JavaSparkMain) plugin, caller);
    } else if (plugin instanceof SparkMain) {
      return new WrappedSparkMain((SparkMain) plugin, caller);
    }

    return plugin;
  }

}

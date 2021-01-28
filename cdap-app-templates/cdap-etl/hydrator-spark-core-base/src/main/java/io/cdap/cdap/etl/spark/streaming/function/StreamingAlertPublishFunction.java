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

package io.cdap.cdap.etl.spark.streaming.function;

import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.AlertPublisherContext;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultAlertPublisherContext;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.TrackedIterator;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkPipelineRuntime;
import io.cdap.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function used to publish alerts with a JavaDStream.
 */
public class StreamingAlertPublishFunction implements Function2<JavaRDD<Alert>, Time, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingAlertPublishFunction.class);
  private final JavaSparkExecutionContext sec;
  private final StageSpec stageSpec;

  public StreamingAlertPublishFunction(JavaSparkExecutionContext sec, StageSpec stageSpec) {
    this.sec = sec;
    this.stageSpec = stageSpec;
  }

  @Override
  public Void call(JavaRDD<Alert> data, Time batchTime) throws Exception {
    MacroEvaluator evaluator = new DefaultMacroEvaluator(new BasicArguments(sec),
                                                         batchTime.milliseconds(),
                                                         sec.getSecureStore(),
                                                         sec.getServiceDiscoverer(),
                                                         sec.getNamespace());

    PluginContext pluginContext = new SparkPipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                 stageSpec.isStageLoggingEnabled(),
                                                                 stageSpec.isProcessTimingEnabled());

    String stageName = stageSpec.getName();
    AlertPublisher alertPublisher = pluginContext.newPluginInstance(stageName, evaluator);
    PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec, batchTime.milliseconds());

    AlertPublisherContext alertPublisherContext =
      new DefaultAlertPublisherContext(pipelineRuntime, stageSpec, sec.getMessagingContext(), sec.getAdmin());
    alertPublisher.initialize(alertPublisherContext);

    StageMetrics stageMetrics = new DefaultStageMetrics(sec.getMetrics(), stageName);
    TrackedIterator<Alert> trackedAlerts =
      new TrackedIterator<>(data.collect().iterator(), stageMetrics, Constants.Metrics.RECORDS_IN);
    alertPublisher.publish(trackedAlerts);
    alertPublisher.destroy();
    return null;
  }
}

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

package co.cask.cdap.etl.spark.streaming.function;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spark.batch.BasicSparkPluginContext;
import co.cask.cdap.etl.spark.function.CountingFunction;
import co.cask.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import co.cask.cdap.etl.spark.streaming.SparkStreamingExecutionContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function used to write a batch of data to a {@link SparkSink} for use with a JavaDStream.
 *
 * @param <T> type of object in the rdd
 */
public class StreamingSparkSinkFunction<T> implements Function2<JavaRDD<T>, Time, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingSparkSinkFunction.class);
  private final JavaSparkExecutionContext sec;
  private final StageInfo stageInfo;

  public StreamingSparkSinkFunction(JavaSparkExecutionContext sec, StageInfo stageInfo) {
    this.sec = sec;
    this.stageInfo = stageInfo;
  }

  @Override
  public Void call(JavaRDD<T> data, Time batchTime) throws Exception {
    final long logicalStartTime = batchTime.milliseconds();
    MacroEvaluator evaluator = new DefaultMacroEvaluator(sec.getWorkflowToken(),
                                                         sec.getRuntimeArguments(),
                                                         logicalStartTime,
                                                         sec.getSecureStore(),
                                                         sec.getNamespace());

    final PluginContext pluginContext = new SparkPipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                       stageInfo.isStageLoggingEnabled(),
                                                                       stageInfo.isProcessTimingEnabled());

    final String stageName = stageInfo.getName();
    final SparkSink<T> sparkSink = pluginContext.newPluginInstance(stageName, evaluator);
    boolean isPrepared = false;
    boolean isDone = false;

    try {
      sec.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          SparkPluginContext context = new BasicSparkPluginContext(sec, datasetContext, stageInfo);
          sparkSink.prepareRun(context);
        }
      });
      isPrepared = true;

      final SparkExecutionPluginContext sparkExecutionPluginContext
        = new SparkStreamingExecutionContext(sec, JavaSparkContext.fromSparkContext(data.rdd().context()),
                                             logicalStartTime, stageInfo);
      final JavaRDD<T> countedRDD = data.map(new CountingFunction<T>(stageName, sec.getMetrics(),
                                                                     "records.in", null)).cache();
      sec.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          sparkSink.run(sparkExecutionPluginContext, countedRDD);
        }
      });
      isDone = true;
      sec.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          SparkPluginContext context = new BasicSparkPluginContext(sec, datasetContext, stageInfo);
          sparkSink.onRunFinish(true, context);
        }
      });
    } catch (Exception e) {
      LOG.error("Error while executing sink {} for the batch for time {}.", stageName, logicalStartTime, e);
    } finally {
      if (isPrepared && !isDone) {
        sec.execute(new TxRunnable() {
          @Override
          public void run(DatasetContext datasetContext) throws Exception {
            SparkPluginContext context = new BasicSparkPluginContext(sec, datasetContext, stageInfo);
            sparkSink.onRunFinish(false, context);
          }
        });
      }
    }
    return null;
  }
}

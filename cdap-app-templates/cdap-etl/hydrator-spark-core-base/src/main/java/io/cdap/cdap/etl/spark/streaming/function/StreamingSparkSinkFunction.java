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

import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkPipelineRuntime;
import io.cdap.cdap.etl.spark.batch.BasicSparkPluginContext;
import io.cdap.cdap.etl.spark.function.CountingFunction;
import io.cdap.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import io.cdap.cdap.etl.spark.streaming.SparkStreamingExecutionContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Function used to write a batch of data to a {@link SparkSink} for use with a JavaDStream.
 *
 * @param <T> type of object in the rdd
 */
public class StreamingSparkSinkFunction<T> implements VoidFunction2<JavaRDD<T>, Time> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingSparkSinkFunction.class);
  private final JavaSparkExecutionContext sec;
  private final StageSpec stageSpec;
  @Nullable
  private final DatasetContext datasetContext;

  public StreamingSparkSinkFunction(JavaSparkExecutionContext sec, StageSpec stageSpec,
                                    @Nullable DatasetContext datasetContext) {
    this.sec = sec;
    this.stageSpec = stageSpec;
    this.datasetContext = datasetContext;
  }

  @Override
  public void call(JavaRDD<T> data, Time batchTime) throws Exception {
    if (data.isEmpty()) {
      return;
    }

    final long logicalStartTime = batchTime.milliseconds();
    MacroEvaluator evaluator = new DefaultMacroEvaluator(new BasicArguments(sec),
                                                         logicalStartTime,
                                                         sec.getSecureStore(),
                                                         sec.getServiceDiscoverer(),
                                                         sec.getNamespace());

    final PluginContext pluginContext = new SparkPipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                       stageSpec.isStageLoggingEnabled(),
                                                                       stageSpec.isProcessTimingEnabled());
    final PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec, batchTime.milliseconds());

    final String stageName = stageSpec.getName();
    final SparkSink<T> sparkSink = pluginContext.newPluginInstance(stageName, evaluator);
    boolean isPrepared = false;
    boolean isDone = false;

    try {
      prepareRun(pipelineRuntime, sparkSink);
      isPrepared = true;

      final SparkExecutionPluginContext sparkExecutionPluginContext
        = new SparkStreamingExecutionContext(sec, JavaSparkContext.fromSparkContext(data.rdd().context()),
                                             logicalStartTime, stageSpec);
      final JavaRDD<T> countedRDD = data.map(new CountingFunction<T>(stageName, sec.getMetrics(),
                                                                     "records.in", null)).cache();
      runSparkSink(sparkSink, sparkExecutionPluginContext, countedRDD);
      isDone = true;
      onRunFinish(pipelineRuntime, sparkSink, true);
    } catch (Exception e) {
      LOG.error("Error while executing sink {} for the batch for time {}.", stageName, logicalStartTime, e);
    } finally {
      if (isPrepared && !isDone) {
        onRunFinish(pipelineRuntime, sparkSink, false);
      }
    }
  }

  private void onRunFinish(PipelineRuntime pipelineRuntime, SparkSink<T> sparkSink,
                           boolean succeeded) throws TransactionFailureException {
    if (datasetContext != null) {
      onRunFinishWithContext(datasetContext, pipelineRuntime, sparkSink, succeeded);
      return;
    }
    sec.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        onRunFinishWithContext(datasetContext, pipelineRuntime, sparkSink, succeeded);
      }
    });
  }

  private void onRunFinishWithContext(DatasetContext datasetContext, PipelineRuntime pipelineRuntime,
                                      SparkSink<T> sparkSink, boolean succeeded) {
    SparkPluginContext context = new BasicSparkPluginContext(null, pipelineRuntime, stageSpec,
                                                             datasetContext, sec.getAdmin());
    sparkSink.onRunFinish(succeeded, context);
  }

  private void runSparkSink(SparkSink<T> sparkSink, SparkExecutionPluginContext sparkExecutionPluginContext,
                            JavaRDD<T> countedRDD) throws Exception {
    if (datasetContext != null) {
      //already in a transaction
      sparkSink.run(sparkExecutionPluginContext, countedRDD);
      return;
    }
    sec.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        sparkSink.run(sparkExecutionPluginContext, countedRDD);
      }
    });
  }

  private void prepareRun(PipelineRuntime pipelineRuntime, SparkSink<T> sparkSink) throws Exception {
    if (datasetContext != null) {
      prepareRunWithContext(datasetContext, pipelineRuntime, sparkSink);
      return;
    }
    sec.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        prepareRunWithContext(datasetContext, pipelineRuntime, sparkSink);
      }
    });
  }

  private void prepareRunWithContext(DatasetContext datasetContext, PipelineRuntime pipelineRuntime,
                                     SparkSink<T> sparkSink) throws Exception {
    SparkPluginContext context = new BasicSparkPluginContext(null, pipelineRuntime, stageSpec,
                                                             datasetContext, sec.getAdmin());
    sparkSink.prepareRun(context);
  }
}

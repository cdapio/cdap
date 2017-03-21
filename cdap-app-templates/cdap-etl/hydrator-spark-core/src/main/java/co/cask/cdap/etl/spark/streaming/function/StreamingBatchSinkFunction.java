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

package co.cask.cdap.etl.spark.streaming.function;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spark.batch.SparkBatchSinkContext;
import co.cask.cdap.etl.spark.batch.SparkBatchSinkFactory;
import co.cask.cdap.etl.spark.function.CountingFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function used to write a batch of data to a batch sink for use with a JavaDStream.
 * note: not using foreachRDD(VoidFunction2) method, because spark 1.3 doesn't have VoidFunction2.
 *
 * @param <T> type of object in the rdd
 */
public class StreamingBatchSinkFunction<T> implements Function2<JavaRDD<T>, Time, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingBatchSinkFunction.class);
  private final PairFlatMapFunction<T, Object, Object> sinkFunction;
  private final JavaSparkExecutionContext sec;
  private final StageInfo stageInfo;

  public StreamingBatchSinkFunction(PairFlatMapFunction<T, Object, Object> sinkFunction,
                                    JavaSparkExecutionContext sec, StageInfo stageInfo) {
    this.sinkFunction = sinkFunction;
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
    final SparkBatchSinkFactory sinkFactory = new SparkBatchSinkFactory();
    final String stageName = stageInfo.getName();
    final BatchSink<Object, Object, Object> batchSink =
      sec.getPluginContext().newPluginInstance(stageName, evaluator);
    boolean isPrepared = false;
    boolean isDone = false;

    try {
      sec.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          SparkBatchSinkContext sinkContext =
            new SparkBatchSinkContext(sinkFactory, sec, datasetContext, logicalStartTime, stageInfo,
                                      sec.getDataTracer(stageName).isEnabled());
          batchSink.prepareRun(sinkContext);
        }
      });
      isPrepared = true;

      data = data.map(new CountingFunction<T>(stageName, sec.getMetrics(), "records.in", null));
      sinkFactory.writeFromRDD(data.flatMapToPair(sinkFunction), sec, stageName, Object.class, Object.class);
      isDone = true;
      sec.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext datasetContext) throws Exception {
          SparkBatchSinkContext sinkContext =
            new SparkBatchSinkContext(sinkFactory, sec, datasetContext, logicalStartTime, stageInfo,
                                      sec.getDataTracer(stageName).isEnabled());
          batchSink.onRunFinish(true, sinkContext);
        }
      });
    } catch (Exception e) {
      LOG.error("Error writing to sink {} for the batch for time {}.", stageName, logicalStartTime, e);
    } finally {
      if (isPrepared && !isDone) {
        sec.execute(new TxRunnable() {
          @Override
          public void run(DatasetContext datasetContext) throws Exception {
            SparkBatchSinkContext sinkContext =
              new SparkBatchSinkContext(sinkFactory, sec, datasetContext, logicalStartTime, stageInfo,
                                        sec.getDataTracer(stageName).isEnabled());
            batchSink.onRunFinish(false, sinkContext);
          }
        });
      }
    }
    return null;
  }
}

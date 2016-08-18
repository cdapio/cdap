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

package co.cask.cdap.etl.spark.streaming;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPairCollection;
import co.cask.cdap.etl.spark.batch.BasicSparkExecutionPluginContext;
import co.cask.cdap.etl.spark.batch.SparkBatchSinkContext;
import co.cask.cdap.etl.spark.batch.SparkBatchSinkFactory;
import co.cask.cdap.etl.spark.function.CountingFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JavaDStream backed {@link co.cask.cdap.etl.spark.SparkCollection}
 *
 * @param <T> type of objects in the collection
 */
public class DStreamCollection<T> implements SparkCollection<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DStreamCollection.class);
  private final JavaSparkExecutionContext sec;
  private final JavaSparkContext sparkContext;
  private final JavaDStream<T> stream;

  public DStreamCollection(JavaSparkExecutionContext sec, JavaSparkContext sparkContext, JavaDStream<T> stream) {
    this.sec = sec;
    this.sparkContext = sparkContext;
    this.stream = stream;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaDStream<T> getUnderlying() {
    return stream;
  }

  @Override
  public SparkCollection<T> cache() {
    return wrap(stream.cache());
  }

  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> union(SparkCollection<T> other) {
    return wrap(stream.union((JavaDStream<T>) other.getUnderlying()));
  }

  @Override
  public <U> SparkCollection<U> flatMap(FlatMapFunction<T, U> function) {
    return wrap(stream.flatMap(function));
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
    return new PairDStreamCollection<>(sec, sparkContext, stream.flatMapToPair(function));
  }

  @Override
  public <U> SparkCollection<U> compute(final String stageName, final SparkCompute<T, U> compute) throws Exception {
    sec.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        SparkExecutionPluginContext sparkPluginContext =
          new BasicSparkExecutionPluginContext(sec, sparkContext, datasetContext, stageName);
        compute.initialize(sparkPluginContext);
      }
    });
    return wrap(stream.transform(new Function2<JavaRDD<T>, Time, JavaRDD<U>>() {
                  @Override
                  public JavaRDD<U> call(JavaRDD<T> data, Time batchTime) throws Exception {
                    SparkExecutionPluginContext sparkPluginContext =
                      new SparkStreamingExecutionContext(sec, sparkContext, stageName, batchTime.milliseconds());

                    data = data.map(new CountingFunction<T>(stageName, sec.getMetrics(), "records.in"));
                    return compute.transform(sparkPluginContext, data)
                      .map(new CountingFunction<U>(stageName, sec.getMetrics(), "records.out"));
                  }
                }));
  }

  @Override
  public void store(final String stageName, final PairFlatMapFunction<T, Object, Object> sinkFunction) {
    // note: not using foreachRDD(VoidFunction2) method, because spark 1.3 doesn't have VoidFunction2
    stream.foreachRDD(new Function2<JavaRDD<T>, Time, Void>() {
      @Override
      public Void call(JavaRDD<T> data, Time batchTime) throws Exception {
        final long logicalStartTime = batchTime.milliseconds();
        MacroEvaluator evaluator = new DefaultMacroEvaluator(sec.getWorkflowToken(),
                                                             sec.getRuntimeArguments(),
                                                             logicalStartTime,
                                                             sec.getSecureStore(),
                                                             sec.getNamespace());
        final SparkBatchSinkFactory sinkFactory = new SparkBatchSinkFactory();
        final BatchSink<Object, Object, Object> batchSink =
          sec.getPluginContext().newPluginInstance(stageName, evaluator);
        boolean isPrepared = false;
        boolean isDone = false;

        try {
          sec.execute(new TxRunnable() {
            @Override
            public void run(DatasetContext datasetContext) throws Exception {
              SparkBatchSinkContext sinkContext =
                new SparkBatchSinkContext(sinkFactory, sec, datasetContext, stageName, logicalStartTime);
              batchSink.prepareRun(sinkContext);
            }
          });
          isPrepared = true;

          data = data.map(new CountingFunction<T>(stageName, sec.getMetrics(), "records.in"));
          sinkFactory.writeFromRDD(data.flatMapToPair(sinkFunction), sec, stageName, Object.class, Object.class);
          isDone = true;
          sec.execute(new TxRunnable() {
            @Override
            public void run(DatasetContext datasetContext) throws Exception {
              SparkBatchSinkContext sinkContext =
                new SparkBatchSinkContext(sinkFactory, sec, datasetContext, stageName, logicalStartTime);
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
                  new SparkBatchSinkContext(sinkFactory, sec, datasetContext, stageName, logicalStartTime);
                batchSink.onRunFinish(false, sinkContext);
              }
            });
          }
        }
        return null;
      }
    });
  }

  @Override
  public void store(String stageName, SparkSink<T> sink) throws Exception {
    // should never be called.
    throw new UnsupportedOperationException("Spark sink not supported in Spark Streaming.");
  }

  @Override
  public SparkCollection<T> window(final String stageName, Windower windower) {
    return wrap(stream
                  .transform(new Function<JavaRDD<T>, JavaRDD<T>>() {
                    @Override
                    public JavaRDD<T> call(JavaRDD<T> in) throws Exception {
                      return in.map(new CountingFunction<T>(stageName, sec.getMetrics(), "records.in"));
                    }
                  })
                  .window(Durations.seconds(windower.getWidth()), Durations.seconds(windower.getSlideInterval()))
                  .transform(new Function<JavaRDD<T>, JavaRDD<T>>() {
                    @Override
                    public JavaRDD<T> call(JavaRDD<T> in) throws Exception {
                      return in.map(new CountingFunction<T>(stageName, sec.getMetrics(), "records.out"));
                    }
                  }));
  }

  private <U> SparkCollection<U> wrap(JavaDStream<U> stream) {
    return new DStreamCollection<>(sec, sparkContext, stream);
  }
}

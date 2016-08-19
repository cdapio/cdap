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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPairCollection;
import co.cask.cdap.etl.spark.function.CountingFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;


/**
 * Implementation of {@link SparkCollection} that is backed by a JavaRDD.
 *
 * @param <T> type of object in the collection
 */
public class RDDCollection<T> implements SparkCollection<T> {
  private final JavaSparkExecutionContext sec;
  private final JavaSparkContext jsc;
  private final DatasetContext datasetContext;
  private final SparkBatchSinkFactory sinkFactory;
  private final JavaRDD<T> rdd;

  public RDDCollection(JavaSparkExecutionContext sec, JavaSparkContext jsc,
                       DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory, JavaRDD<T> rdd) {
    this.sec = sec;
    this.jsc = jsc;
    this.datasetContext = datasetContext;
    this.sinkFactory = sinkFactory;
    this.rdd = rdd;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaRDD<T> getUnderlying() {
    return rdd;
  }

  @Override
  public SparkCollection<T> cache() {
    return wrap(rdd.cache());
  }

  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> union(SparkCollection<T> other) {
    return wrap(rdd.union((JavaRDD<T>) other.getUnderlying()));
  }

  @Override
  public <U> SparkCollection<U> flatMap(FlatMapFunction<T, U> function) {
    return wrap(rdd.flatMap(function));
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
    return new PairRDDCollection<>(sec, jsc, datasetContext, sinkFactory, rdd.flatMapToPair(function));
  }

  @Override
  public <U> SparkCollection<U> compute(String stageName, SparkCompute<T, U> compute) throws Exception {
    SparkExecutionPluginContext sparkPluginContext =
      new BasicSparkExecutionPluginContext(sec, jsc, datasetContext, stageName);
    compute.initialize(sparkPluginContext);

    JavaRDD<T> countedInput = rdd.map(new CountingFunction<T>(stageName, sec.getMetrics(), "records.in")).cache();

    return wrap(compute.transform(sparkPluginContext, countedInput)
                  .map(new CountingFunction<U>(stageName, sec.getMetrics(), "records.out")));
  }

  @Override
  public void store(String stageName, PairFlatMapFunction<T, Object, Object> sinkFunction) {
    JavaPairRDD<Object, Object> sinkRDD = rdd.flatMapToPair(sinkFunction);
    sinkFactory.writeFromRDD(sinkRDD, sec, stageName, Object.class, Object.class);
  }

  @Override
  public void store(String stageName, SparkSink<T> sink) throws Exception {
    SparkExecutionPluginContext sparkPluginContext =
      new BasicSparkExecutionPluginContext(sec, jsc, datasetContext, stageName);

    JavaRDD<T> countedRDD = rdd.map(new CountingFunction<T>(stageName, sec.getMetrics(), "records.in")).cache();
    sink.run(sparkPluginContext, countedRDD);
  }

  @Override
  public SparkCollection<T> window(String stageName, Windower windower) {
    throw new UnsupportedOperationException("Windowing is not supported on RDDs.");
  }

  private <U> RDDCollection<U> wrap(JavaRDD<U> rdd) {
    return new RDDCollection<>(sec, jsc, datasetContext, sinkFactory, rdd);
  }
}

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

import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.spark.function.CountingFunction;
import co.cask.cdap.etl.spark.streaming.SparkStreamingExecutionContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

/**
 * Function used to implement a SparkCompute stage in a DStream.
 *
 * @param <T> type of object in the input rdd
 * @param <U> type of object in the output rdd
 */
public class ComputeTransformFunction<T, U> implements Function2<JavaRDD<T>, Time, JavaRDD<U>> {
  private final JavaSparkExecutionContext sec;
  private final String stageName;
  private final SparkCompute<T, U> compute;

  public ComputeTransformFunction(JavaSparkExecutionContext sec, String stageName,
                                  SparkCompute<T, U> compute) {
    this.sec = sec;
    this.stageName = stageName;
    this.compute = compute;
  }

  @Override
  public JavaRDD<U> call(JavaRDD<T> data, Time batchTime) throws Exception {
    SparkExecutionPluginContext sparkPluginContext =
      new SparkStreamingExecutionContext(sec, JavaSparkContext.fromSparkContext(data.context()),
                                         stageName, batchTime.milliseconds());

    data = data.map(new CountingFunction<T>(stageName, sec.getMetrics(), "records.in", null));
    return compute.transform(sparkPluginContext, data)
      .map(new CountingFunction<U>(stageName, sec.getMetrics(), "records.out", sec.getDataTracer(stageName)));
  }
}

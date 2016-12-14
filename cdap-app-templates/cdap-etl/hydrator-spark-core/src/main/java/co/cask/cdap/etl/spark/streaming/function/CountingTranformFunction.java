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

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.etl.spark.function.CountingFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import javax.annotation.Nullable;

/**
 * Function used to emit a metric for every item in an RDD.
 *
 * @param <T> type of object in the rdd.
 */
public class CountingTranformFunction<T> implements Function<JavaRDD<T>, JavaRDD<T>> {
  private final Metrics metrics;
  private final String stageName;
  private final String metricName;
  private final DataTracer dataTracer;

  // DataTracer is null for records.in
  public CountingTranformFunction(String stageName, Metrics metrics, String metricName,
                                  @Nullable DataTracer dataTracer) {
    this.metrics = metrics;
    this.stageName = stageName;
    this.metricName = metricName;
    this.dataTracer = dataTracer;
  }

  @Override
  public JavaRDD<T> call(JavaRDD<T> in) throws Exception {
    return in.map(new CountingFunction<T>(stageName, metrics, metricName, dataTracer));
  }
}

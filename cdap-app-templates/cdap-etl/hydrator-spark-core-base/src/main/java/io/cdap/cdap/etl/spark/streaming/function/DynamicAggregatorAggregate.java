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

package io.cdap.cdap.etl.spark.streaming.function;

import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.spark.function.AggregatorAggregateFunction;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.streaming.DynamicDriverContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import scala.Tuple2;

/**
 * Serializable function that can be used to perform the aggregate part of an Aggregator. Dynamically instantiates
 * the Aggregator plugin used to ensure that code changes are picked up and to ensure that macro substitution occurs.
 *
 * @param <GROUP_KEY> type of group key
 * @param <GROUP_VAL> type of group val
 * @param <OUT> type of output object
 */
public class DynamicAggregatorAggregate<GROUP_KEY, GROUP_VAL, OUT>
  implements Function2<JavaPairRDD<GROUP_KEY, Iterable<GROUP_VAL>>, Time, JavaRDD<RecordInfo<Object>>> {
  private final DynamicDriverContext dynamicDriverContext;
  private final FunctionCache functionCache;
  private transient FlatMapFunction<Tuple2<GROUP_KEY, Iterable<GROUP_VAL>>, RecordInfo<Object>> function;

  public DynamicAggregatorAggregate(DynamicDriverContext dynamicDriverContext, FunctionCache functionCache) {
    this.dynamicDriverContext = dynamicDriverContext;
    this.functionCache = functionCache;
  }

  @Override
  public JavaRDD<RecordInfo<Object>> call(JavaPairRDD<GROUP_KEY, Iterable<GROUP_VAL>> input,
                                          Time batchTime) throws Exception {
    if (function == null) {
      function = new AggregatorAggregateFunction<GROUP_KEY, GROUP_VAL, OUT>(
        dynamicDriverContext.getPluginFunctionContext(), functionCache);
    }
    return input.flatMap(function);
  }
}

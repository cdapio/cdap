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

import co.cask.cdap.etl.spark.function.AggregatorGroupByFunction;
import co.cask.cdap.etl.spark.streaming.DynamicDriverContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

/**
 * Serializable function that can be used to perform the group by part of an Aggregator. Dynamically instantiates
 * the Aggregator plugin used to ensure that code changes are picked up and to ensure that macro substitution occurs.
 *
 * @param <GROUP_KEY> type of group key
 * @param <GROUP_VAL> type of group val
 */
public class DynamicAggregatorGroupBy<GROUP_KEY, GROUP_VAL>
  implements Function2<JavaRDD<GROUP_VAL>, Time, JavaPairRDD<GROUP_KEY, GROUP_VAL>> {
  private final DynamicDriverContext dynamicDriverContext;
  private transient AggregatorGroupByFunction<GROUP_KEY, GROUP_VAL> aggregatorGroupByFunction;

  public DynamicAggregatorGroupBy(DynamicDriverContext dynamicDriverContext) {
    this.dynamicDriverContext = dynamicDriverContext;
  }

  @Override
  public JavaPairRDD<GROUP_KEY, GROUP_VAL> call(JavaRDD<GROUP_VAL> input, Time batchTime) throws Exception {
    if (aggregatorGroupByFunction == null) {
      aggregatorGroupByFunction = new AggregatorGroupByFunction<>(dynamicDriverContext.getPluginFunctionContext());
    }
    return input.flatMapToPair(aggregatorGroupByFunction);
  }
}

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

import co.cask.cdap.etl.spark.function.JoinOnFunction;
import co.cask.cdap.etl.spark.streaming.DynamicDriverContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

/**
 * Serializable function that can be used to perform the joinOn part of a Joiner. Dynamically instantiates
 * the relevant Joiner plugin to ensure that code changes are picked up and to ensure
 * that macro substitution occurs.
 *
 * @param <JOIN_KEY> type of join key
 * @param <T> type of input object
 */
public class DynamicJoinOn<JOIN_KEY, T> implements Function2<JavaRDD<T>, Time, JavaPairRDD<JOIN_KEY, T>> {
  private final DynamicDriverContext dynamicDriverContext;
  private final String inputStageName;
  private transient JoinOnFunction<JOIN_KEY, T> function;

  public DynamicJoinOn(DynamicDriverContext dynamicDriverContext, String inputStageName) {
    this.dynamicDriverContext = dynamicDriverContext;
    this.inputStageName = inputStageName;
  }

  @Override
  public JavaPairRDD<JOIN_KEY, T> call(JavaRDD<T> input, Time batchTime) throws Exception {
    if (function == null) {
      function = new JoinOnFunction<>(dynamicDriverContext.getPluginFunctionContext(), inputStageName);
    }
    return input.flatMapToPair(function);
  }
}

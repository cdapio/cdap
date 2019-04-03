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

import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.spark.Compat;
import co.cask.cdap.etl.spark.function.JoinMergeFunction;
import co.cask.cdap.etl.spark.streaming.DynamicDriverContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import scala.Tuple2;

import java.util.List;

/**
 * Serializable function that can be used to perform the merge part of a Joiner. Dynamically instantiates
 * the relevant Joiner plugin to ensure that code changes are picked up and to ensure
 * that macro substitution occurs.
 *
 * @param <JOIN_KEY> type of join key
 * @param <INPUT_RECORD> type of input object
 * @param <OUT> type of output object
 */
public class DynamicJoinMerge<JOIN_KEY, INPUT_RECORD, OUT>
  implements Function2<JavaPairRDD<JOIN_KEY, List<JoinElement<INPUT_RECORD>>>, Time, JavaRDD<OUT>> {
  private final DynamicDriverContext dynamicDriverContext;
  private transient FlatMapFunction<Tuple2<JOIN_KEY, List<JoinElement<INPUT_RECORD>>>, OUT> function;

  public DynamicJoinMerge(DynamicDriverContext dynamicDriverContext) {
    this.dynamicDriverContext = dynamicDriverContext;
  }
  
  @Override
  public JavaRDD<OUT> call(JavaPairRDD<JOIN_KEY, List<JoinElement<INPUT_RECORD>>> input,
                           Time batchTime) throws Exception {
    if (function == null) {
      function = Compat.convert(
        new JoinMergeFunction<JOIN_KEY, INPUT_RECORD, OUT>(dynamicDriverContext.getPluginFunctionContext()));
    }
    return input.flatMap(function);
  }

}

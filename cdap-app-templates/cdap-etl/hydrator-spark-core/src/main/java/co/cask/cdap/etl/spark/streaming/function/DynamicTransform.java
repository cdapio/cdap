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

import co.cask.cdap.etl.spark.function.TransformFunction;
import co.cask.cdap.etl.spark.streaming.DynamicDriverContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

/**
 * Serializable function that can be used to perform a flat map on a DStream. Dynamically instantiates
 * the Transform plugin used to perform the flat map to ensure that code changes are picked up and to ensure
 * that macro substitution occurs.
 *
 * @param <T> type of input object
 * @param <U> type of output object
 */
public class DynamicTransform<T, U> implements Function2<JavaRDD<T>, Time, JavaRDD<U>> {
  private final DynamicDriverContext dynamicDriverContext;
  private transient TransformFunction<T, U> transformFunction;

  public DynamicTransform(DynamicDriverContext dynamicDriverContext) {
    this.dynamicDriverContext = dynamicDriverContext;
  }

  @Override
  public JavaRDD<U> call(JavaRDD<T> input, Time batchTime) throws Exception {
    if (transformFunction == null) {
      transformFunction = new TransformFunction<>(dynamicDriverContext.getPluginFunctionContext());
    }
    return input.flatMap(transformFunction);
  }
}

/*
 * Copyright © 2017 Cask Data, Inc.
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

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Simply wraps all elements into a Tuple2 whose first element is false, indicating everything is output and nothing
 * is an error.
 *
 * @param <T> the type of output object
 */
public class WrapOutputTransformFunction<T> implements Function<T, Tuple2<Boolean, T>> {
  @Override
  public Tuple2<Boolean, T> call(T inputRecord) throws Exception {
    return new Tuple2<>(false, inputRecord);
  }
}

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

package io.cdap.cdap.etl.spark.function;

import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.common.DefaultEmitter;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Function that uses a supplied {@link Transform} to transform a pair into a record.
 *
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <OUT> The type of the Output Object
 * @param <IN_KEY> the type of the input key
 * @param <IN_VAL> the type of the input value
 */
public class TransformFromPairFunction<OUT, IN_KEY, IN_VAL> implements FlatMapFunction<Tuple2<IN_KEY, IN_VAL>, OUT> {
  private final Transform<KeyValue<IN_KEY, IN_VAL>, OUT> transform;
  private transient DefaultEmitter<OUT> emitter;

  public TransformFromPairFunction(Transform<KeyValue<IN_KEY, IN_VAL>, OUT> transform) {
    this.transform = transform;
  }

  @Override
  public Iterator<OUT> call(Tuple2<IN_KEY, IN_VAL> input) throws Exception {
    if (emitter == null) {
      emitter = new DefaultEmitter<>();
    }
    transform.transform(new KeyValue<>(input._1(), input._2()), emitter);
    return emitter.getEntries().iterator();
  }
}

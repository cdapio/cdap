/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.common.base.Function;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.common.TrackedTransform;
import io.cdap.cdap.etl.common.TransformingEmitter;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Function that uses a supplied {@link Transform} to transform one record into a pair.
 *
 * Non-serializable fields are lazily created since this is used in a Spark closure.
 *
 * @param <IN> type of input object
 * @param <OUT_KEY> type of output key
 * @param <OUT_VAL> type of output val
 */
public class TransformToPairFunction<IN, OUT_KEY, OUT_VAL> implements PairFlatMapFunction<IN, OUT_KEY, OUT_VAL> {
  private final Transform<IN, KeyValue<OUT_KEY, OUT_VAL>> transform;
  private transient TransformingEmitter<KeyValue<OUT_KEY, OUT_VAL>, Tuple2<OUT_KEY, OUT_VAL>> emitter;

  public TransformToPairFunction(Transform<IN, KeyValue<OUT_KEY, OUT_VAL>> transform) {
    this.transform = transform;
  }

  @Override
  public Iterator<Tuple2<OUT_KEY, OUT_VAL>> call(IN input) throws Exception {
    if (emitter == null) {
      emitter = new TransformingEmitter<>(new Function<KeyValue<OUT_KEY, OUT_VAL>, Tuple2<OUT_KEY, OUT_VAL>>() {
        @Override
        public Tuple2<OUT_KEY, OUT_VAL> apply(KeyValue<OUT_KEY, OUT_VAL> input) {
          return new Tuple2<>(input.getKey(), input.getValue());
        }
      });
    }
    emitter.reset();
    transform.transform(input, emitter);
    return emitter.getEntries().iterator();
  }
}

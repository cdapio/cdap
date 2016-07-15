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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transformation;

/**
 *
 * @param <IN>
 * @param <OUT>
 */
public class KVSourceTransformation<IN, OUT> implements Transformation<IN, KeyValue<String, OUT>> {
  private final Transformation<IN, OUT> transformation;
  private final String stageName;


  public KVSourceTransformation(Transformation<IN, OUT> transformation, String stageName) {
    this.transformation = transformation;
    this.stageName = stageName;
  }

  @Override
  public void transform(IN input, Emitter<KeyValue<String, OUT>> emitter) throws Exception {
    DefaultEmitter<OUT> singleEmitter = new DefaultEmitter<>();
    transformation.transform(input, singleEmitter);
    for (OUT out : singleEmitter.getEntries()) {
      emitter.emit(new KeyValue<>(stageName, out));
    }
  }
}

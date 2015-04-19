/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.templates.etl.api.Transform;

import java.util.List;

/**
 * Execution of Transforms one iteration at a time.
 *
 * @param <IN> the type of input object to the first transform
 * @param <OUT> the type of object output by the last transform
 */
public class TransformExecutor<IN, OUT> {
  private final List<Transform> transformList;
  private DefaultEmitter previousEmitter;
  private DefaultEmitter currentEmitter;

  public TransformExecutor(List<Transform> transforms) {
    this.transformList = transforms;
    this.previousEmitter = new DefaultEmitter();
    this.currentEmitter = new DefaultEmitter();
  }

  public Iterable<OUT> runOneIteration(IN input) throws Exception {
    previousEmitter.reset();
    previousEmitter.emit(input);
    for (Transform transform : transformList) {
      for (Object transformedVal : previousEmitter) {
        transform.transform(transformedVal, currentEmitter);
      }
      previousEmitter.reset();
      DefaultEmitter temp = previousEmitter;
      previousEmitter = currentEmitter;
      currentEmitter = temp;
    }
    return previousEmitter;
  }
}

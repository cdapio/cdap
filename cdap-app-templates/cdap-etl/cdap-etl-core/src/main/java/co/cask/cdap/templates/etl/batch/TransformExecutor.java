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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.common.DefaultEmitter;

import java.util.List;
import java.util.Map;

/**
 * Execution of Transforms one iteration at a time.
 */
public final class TransformExecutor {
  private final List<Transform> transformList;
  private DefaultEmitter previousEmitter;
  private DefaultEmitter currentEmitter;

  public TransformExecutor(List<Transform> transforms) {
    this.transformList = transforms;
    this.previousEmitter = new DefaultEmitter();
    this.currentEmitter = new DefaultEmitter();

  }

  public void runOneIteration(DefaultEmitter data) throws Exception {
    //Copy Data into previousEmitter and reset 'data'.
    for (Map.Entry entry : data) {
      previousEmitter.emit(entry.getKey(), entry.getValue());
    }
    data.reset();

    for (Transform transform : transformList) {
      for (Map.Entry entry : previousEmitter) {
        transform.transform(entry.getKey(), entry.getValue(), currentEmitter);
      }
      previousEmitter.reset();
      DefaultEmitter temp = previousEmitter;
      previousEmitter = currentEmitter;
      currentEmitter = temp;
    }

    //Copy result into 'data'
    for (Map.Entry entry : previousEmitter) {
      data.emit(entry.getKey(), entry.getValue());
    }
    previousEmitter.reset();
  }
}

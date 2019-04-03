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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.annotation.Beta;

/**
 * Transforms an input object into zero or more output objects, emitted to specific output ports.
 *
 * @param <IN> Type of input object
 * @param <E> Type of error object
 */
@Beta
public interface MultiOutputTransformation<IN, E> {

  /**
   * Transform the input and emit output using {@link MultiOutputEmitter}.
   *
   * @param input input data to be transformed
   * @param emitter {@link MultiOutputEmitter} to emit data to the next stage
   * @throws Exception if there's an error during this method invocation
   */
  void transform(IN input, MultiOutputEmitter<E> emitter) throws Exception;
}

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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.annotation.Beta;

/**
 * Used to emit one or more key, value pairs to the next stage.
 *
 * @param <T> Type of the object to emit
 */
@Beta
public interface Emitter<T> extends AlertEmitter, ErrorEmitter<T> {

  /**
   * Emit an object. Note that if any further stages has an exception thrown, it will be propagated to this stage.
   * @param value the object to emit
   */
  void emit(T value);

}

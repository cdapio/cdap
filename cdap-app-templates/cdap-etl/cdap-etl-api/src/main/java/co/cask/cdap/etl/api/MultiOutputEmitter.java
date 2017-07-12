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
 * Used to emit one or more key-value pairs to output ports. Values emitted to a port will only be received by
 * stages connected to that specific port.
 *
 * @param <T> Type of error record
 */
@Beta
public interface MultiOutputEmitter<T> {

  /**
   * Emit an output record to the specified port. Only stages connected to that port will receive the record.
   *
   * @param port the port to emit the output record to
   * @param value the output record
   */
  void emit(String port, Object value);

  /**
   * Emit an Error object. If an {@link ErrorTransform} is placed after this stage, it will be able to consume
   * the errors. Otherwise the errors are simply dropped.
   *
   * @param invalidEntry {@link InvalidEntry} representing the error.
   */
  void emitError(InvalidEntry<T> invalidEntry);
}

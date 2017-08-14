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

package co.cask.cdap.etl.common.plugin;

import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.MultiOutputEmitter;

import java.util.Map;

/**
 * An emitter that stops an {@link OperationTimer} when it is emitting something. This is used because when we are
 * timing how long a plugin's method takes, we don't want to include time spent emitting records, especially
 * since the emit call can call subsequent stages before returning.
 *
 * @param <E> the error type
 */
public class UntimedMultiOutputEmitter<E> implements MultiOutputEmitter<E> {
  private final MultiOutputEmitter<E> delegate;
  private final OperationTimer operationTimer;

  public UntimedMultiOutputEmitter(MultiOutputEmitter<E> delegate, OperationTimer operationTimer) {
    this.delegate = delegate;
    this.operationTimer = operationTimer;
  }

  @Override
  public void emitAlert(Map<String, String> payload) {
    operationTimer.stop();
    try {
      delegate.emitAlert(payload);
    } finally {
      operationTimer.start();
    }
  }

  @Override
  public void emitError(InvalidEntry<E> invalidEntry) {
    operationTimer.stop();
    try {
      delegate.emitError(invalidEntry);
    } finally {
      operationTimer.start();
    }
  }

  @Override
  public void emit(String port, Object value) {
    operationTimer.stop();
    try {
      delegate.emit(port, value);
    } finally {
      operationTimer.start();
    }
  }
}

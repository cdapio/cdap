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

package co.cask.cdap.etl.mock.common;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock implementation of {@link Emitter} for unit tests.
 *
 * @param <T> type of object to emit
 */
public class MockEmitter<T> implements Emitter<T> {
  private final List<T> emitted = new ArrayList<>();
  private final List<InvalidEntry<T>> errors = new ArrayList<>();
  private final List<Map<String, String>> alerts = new ArrayList<>();

  @Override
  public void emit(T value) {
    emitted.add(value);
  }

  @Override
  public void emitError(InvalidEntry<T> value) {
    errors.add(value);
  }

  @Override
  public void emitAlert(Map<String, String> payload) {
    alerts.add(new HashMap<>(payload));
  }

  public List<T> getEmitted() {
    return emitted;
  }

  public List<InvalidEntry<T>> getErrors() {
    return errors;
  }

  public List<Map<String, String>> getAlerts() {
    return alerts;
  }

  public void clear() {
    emitted.clear();
    errors.clear();
    alerts.clear();
  }
}

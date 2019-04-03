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

package co.cask.cdap.etl.mock.common;

import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.MultiOutputEmitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock implementation of {@link MultiOutputEmitter} for unit tests.
 *
 * @param <E> type of error object
 */
public class MockMultiOutputEmitter<E> implements MultiOutputEmitter<E> {
  private final Map<String, List<Object>> emitted = new HashMap<>();
  private final List<InvalidEntry<E>> errors = new ArrayList<>();
  private final List<Map<String, String>> alerts = new ArrayList<>();

  @Override
  public void emit(String port, Object value) {
    List<Object> portRecords = emitted.get(port);
    if (portRecords == null) {
      portRecords = new ArrayList<>();
      emitted.put(port, portRecords);
    }
    portRecords.add(value);
  }

  @Override
  public void emitError(InvalidEntry<E> invalidEntry) {
    errors.add(invalidEntry);
  }

  @Override
  public void emitAlert(Map<String, String> payload) {
    alerts.add(new HashMap<>(payload));
  }

  public Map<String, List<Object>> getEmitted() {
    return Collections.unmodifiableMap(emitted);
  }

  public List<InvalidEntry<E>> getErrors() {
    return Collections.unmodifiableList(errors);
  }

  public List<Map<String, String>> getAlerts() {
    return Collections.unmodifiableList(alerts);
  }

  public void clear() {
    emitted.clear();
    errors.clear();
    alerts.clear();
  }
}

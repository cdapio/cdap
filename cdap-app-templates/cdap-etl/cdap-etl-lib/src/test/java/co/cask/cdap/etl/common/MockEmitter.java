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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock emitter for unit tests
 */
public class MockEmitter<T> implements Emitter<T> {
  private final Map<String, List<T>> emitted = new HashMap<>();
  private final Map<String, List<InvalidEntry<T>>> errors = new HashMap<>();

  @Override
  public void emit(String stageName, T value) {
    if (!emitted.containsKey(stageName)) {
      emitted.put(stageName, new ArrayList<T>());
    }
    emitted.get(stageName).add(value);
  }

  @Override
  public void emitError(String stageName, InvalidEntry<T> value) {
    if (!errors.containsKey(stageName)) {
      errors.put(stageName, new ArrayList<InvalidEntry<T>>());
    }
    errors.get(stageName).add(value);
  }

  public List<T> getEmitted(String stageName) {
    return emitted.get(stageName);
  }
  public List<InvalidEntry<T>> getErrors(String stageName) {
    return errors.get(stageName);
  }

  public void clear() {
    emitted.clear();
    errors.clear();
  }
}

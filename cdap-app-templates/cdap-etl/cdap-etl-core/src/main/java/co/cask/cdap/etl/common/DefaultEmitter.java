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

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.StageMetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link Emitter}. Tracks how many records were emitted.
 *
 * @param <T> the type of object to emit
 */
public class DefaultEmitter<T> implements Emitter<T> {
  private final Map<String, List<T>> entriesMap;
  private final Map<String, List<InvalidEntry<T>>> errorMap;
  private final Metrics metrics;

  public DefaultEmitter(Metrics metrics) {
    this.entriesMap = new HashMap<>();
    this.errorMap = new HashMap<>();
    this.metrics = metrics;
  }

  @Override
  public void emit(String stageName, T value) {
    if (!entriesMap.containsKey(stageName)) {
      entriesMap.put(stageName, new ArrayList<T>());
    }
    entriesMap.get(stageName).add(value);
    metrics.count(stageName + ".records.out", 1);
  }

  @Override
  public void emitError(String stageName, InvalidEntry<T> value) {
    if (!errorMap.containsKey(stageName)) {
      errorMap.put(stageName, new ArrayList<InvalidEntry<T>>());
    }
    errorMap.get(stageName).add(value);
    metrics.count(stageName + ".records.errors", 1);
  }

  public List<T> getEntries(String stageName) {
    return entriesMap.get(stageName);
  }

  public Map<String, List<T>> getEntriesMap() {
    return entriesMap;
  }

  public Map<String, List<InvalidEntry<T>>> getErrors() {
    return errorMap;
  }

  public void reset() {
    entriesMap.clear();
    errorMap.clear();
  }
}

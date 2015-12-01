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
import co.cask.cdap.etl.api.InvalidEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default Emitter, that tracks how many records were emitted across stages.
 */
public class DefaultEmitter {
  private final Map<String, List<Object>> entriesMap;
  private final Map<String, List<InvalidEntry<Object>>> errorMap;
  private final Metrics metrics;

  public DefaultEmitter(Metrics metrics) {
    this.entriesMap = new HashMap<>();
    this.errorMap = new HashMap<>();
    this.metrics = metrics;
  }

  public void emit(String stageName, Object value) {
    if (!entriesMap.containsKey(stageName)) {
      entriesMap.put(stageName, new ArrayList<Object>());
    }
    entriesMap.get(stageName).add(value);
    metrics.count(stageName + ".records.out", 1);
  }

  public void emitError(String stageName, InvalidEntry<Object> value) {
    if (!errorMap.containsKey(stageName)) {
      errorMap.put(stageName, new ArrayList<InvalidEntry<Object>>());
    }
    errorMap.get(stageName).add(value);
    metrics.count(stageName + ".records.errors", 1);
  }

  public List<Object> getEntries(String stageName) {
    return entriesMap.get(stageName);
  }

  public Map<String, List<Object>> getEntriesMap() {
    return entriesMap;
  }

  public Map<String, List<InvalidEntry<Object>>> getErrors() {
    return errorMap;
  }

  public void reset() {
    entriesMap.clear();
    errorMap.clear();
  }
}

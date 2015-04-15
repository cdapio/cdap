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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.templates.etl.api.Emitter;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Mock emitter for unit tests
 */
public class MockEmitter<K, V> implements Emitter<K, V> {
  private final List<Entry<K, V>> emitted = Lists.newArrayList();

  @Override
  public void emit(K key, V value) {
    emitted.add(new Entry(key, value));
  }

  public List<Entry<K, V>> getEmitted() {
    return emitted;
  }

  public void clear() {
    emitted.clear();
  }

  public class Entry<K, V> {
    private final K key;
    private final V val;

    public Entry(K key, V val) {
      this.key = key;
      this.val = val;
    }

    public K getKey() {
      return key;
    }

    public V getVal() {
      return val;
    }
  }
}

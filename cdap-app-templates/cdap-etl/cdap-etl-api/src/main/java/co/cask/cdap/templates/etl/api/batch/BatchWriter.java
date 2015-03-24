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

package co.cask.cdap.templates.etl.api.batch;

/**
 * Used to write data to Batch Output.
 *
 * @param <K> Batch Output key class
 * @param <V> Batch Output value class
 */
public interface BatchWriter<K, V> {

  /**
   * Takes in the key and value objects to persist to Batch Output.
   *
   * @param key Key object
   * @param value Value object
   */
  void write(K key, V value);
}

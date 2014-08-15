/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api.dataset.lib;

/**
 * Represents key value pair
 * @param <KEY_TYPE> type of the key
 * @param <VALUE_TYPE> type of the value
 */
public class KeyValue<KEY_TYPE, VALUE_TYPE> {
  private final KEY_TYPE key;
  private final VALUE_TYPE value;

  public KeyValue(KEY_TYPE key, VALUE_TYPE value) {
    this.key = key;
    this.value = value;
  }

  public KEY_TYPE getKey() {
    return key;
  }

  public VALUE_TYPE getValue() {
    return value;
  }
}

/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.api.app;

import java.io.IOException;
import java.util.Optional;

/**
 * Saves and reads state for the given app.
 */
public interface AppStateStore {

  /**
   * Returns the saved state for given app and key.
   *
   * @param key Key for the state, should not be null
   * @return value as Optional<byte[]> . Value will not be present if key is not present in the
   *     store.
   * @throws IOException if the namespace/app is not available or otherwise unable to fetch
   *     state
   * @throws IllegalArgumentException if the key is null or empty
   */
  Optional<byte[]> getState(String key) throws IOException;

  /**
   * Saves the state for an app with a key. This state is removed when the app is deleted.
   *
   * @param key Key for the state, should not be null
   * @param value value as byte[], should not be null
   * @throws IOException if the app is not available or otherwise unable to save state
   * @throws IllegalArgumentException if the key/value is null or empty
   */
  void saveState(String key, byte[] value) throws IOException;

  /**
   * Deletes the state for an app with this key.
   *
   * @param key Key for the state, should not be null
   * @throws IOException if the app is not available or otherwise unable to delete state
   */
  void deleteSate(String key) throws IOException;
}

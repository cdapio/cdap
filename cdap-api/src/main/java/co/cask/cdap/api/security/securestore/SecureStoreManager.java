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

package co.cask.cdap.api.security.securestore;

import java.io.IOException;
import java.util.Map;

/**
 * Provides write access to the secure store.
 * For read access use {@link SecureStore}.
 */
public interface SecureStoreManager {

  /**
   * @param name This is the identifier that will be used to retrieve this element.
   * @param data The sensitive data that has to be securely stored. Passed in as utf-8 formatted byte array
   * @param properties associated with this element
   * @throws IOException If the attempt to store the element failed.
   */
  void put(String name, byte[] data, Map<String, String> properties) throws IOException;

  /**
   * @param name of the element to delete.
   * @throws IOException
   */
  void delete(String name) throws IOException;
}

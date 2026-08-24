/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.security.store;

import io.cdap.cdap.api.annotation.Beta;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides write access to the secure store. For read access use {@link SecureStore}.
 */
@Beta
public interface SecureStoreManager {

  /**
   * Stores an element in the secure store.
   *
   * @param namespace The namespace that this key belongs to
   * @param name This is the identifier that will be used to retrieve this element
   * @param data The sensitive data that has to be securely stored
   * @param description User provided description of the entry
   * @param properties associated with this element
   * @throws IOException If the attempt to store the element failed
   * @throws Exception If the specified namespace does not exist
   */
  void put(String namespace, String name, String data, @Nullable String description,
      Map<String, String> properties) throws Exception;

  /**
   * Stores an element in the secure store with a Time-to-Live (TTL).
   * The default implementation delegates to the standard put, ignoring the TTL.
   *
   * @param namespace The namespace that this key belongs to
   * @param name This is the identifier that will be used to retrieve this element
   * @param data The sensitive data that has to be securely stored
   * @param description User provided description of the entry
   * @param properties associated with this element
   * @param ttlInSeconds Time-To-Live for the secret in seconds.
   * @throws Exception If the attempt to store the element failed
   */
  default void put(String namespace, String name, String data, @Nullable String description,
      Map<String, String> properties, long ttlInSeconds) throws Exception {
    put(namespace, name, data, description, properties);
  }

  /**
   * Deletes the element with the given name.
   *
   * @param namespace The namespace that this key belongs to
   * @param name of the element to delete
   * @throws IOException If the store is not initialized or if the key could not be removed
   * @throws Exception If the specified namespace or name does not exist
   */
  void delete(String namespace, String name) throws Exception;


  /**
   * Attempts to acquire a lease on a secret resource.
   *
   * @param namespace The namespace that this key belongs to
   * @param name Name of the secure key
   * @param timeoutMs lease timeout in milliseconds before lease is considered expired
   * @param leaseHolder The lease holder identity
   * @return true if lease acquired, false otherwise
   * @throws Exception If lease acquisition fails due to underlying storage errors
   */
  default boolean acquireLease(String namespace, String name, long timeoutMs,
                                          String leaseHolder) throws Exception {
    throw new UnsupportedOperationException("Leases are not supported by this SecureStore implementation.");
  }

  /**
   * Releases an acquired lease on a secret resource.
   *
   * @param namespace The namespace that this key belongs to
   * @param name Name of the secure key
   * @param leaseHolder The lease holder identity
   * @throws Exception If lease release fails due to underlying storage errors
   */
  default boolean releaseLease(String namespace, String name, String leaseHolder) throws Exception {
    throw new UnsupportedOperationException("Leases are not supported by this SecureStore implementation.");
  }
}

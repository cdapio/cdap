/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.securestore.spi;

import java.util.Map;

/**
 * Stores and retrieves secret's metadata.
 *
 * @param <T> the type of the metadata to be stored.
 */
public interface SecretsMetadataStore<T> {

  /**
   * Retrieves metadata for the secret for a given namespace.
   *
   * @param namespace the namespace to which secret belongs to
   * @param name the name of the secret to be retrieved
   * @return stored metadata
   * @throws SecretNotFoundException if secret is not found in provided namespace
   * @throws Exception if not able to retrieve the secret in plain text
   */
  T get(String namespace, String name) throws Exception;

  /**
   * Provides list of all the secrets for which metadata is stored.
   *
   * @param namespace the namespace to which secrets belong to
   * @return map of secret to description. If there is no secret metadata stored, empty map is returned
   * @throws Exception if not able to get metadata for the secrets in the provided namespace
   */
  Map<String, String> list(String namespace) throws Exception;

  /**
   * Stores metadata for the secrets
   *
   * @param namespace the namespace to which secret belong to
   * @param name the name of the secret
   * @param metadata the metadata to be stored
   * @throws SecretAlreadyExistsException if the metadata for the secret is already stored
   * @throws Exception if not able to store the metadata for the secret
   */
  void store(String namespace, String name, T metadata) throws Exception;

  /**
   * Deletes the metadata for provided secret.
   *
   * @param namespace the namespace to which secret belongs to
   * @param name the name of the secret to be retrieved
   * @throws SecretNotFoundException if secret is not found in provided namespace
   * @throws Exception
   */
  void delete(String namespace, String name) throws Exception;
}

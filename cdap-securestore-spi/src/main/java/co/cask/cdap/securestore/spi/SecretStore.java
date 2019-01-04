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

import java.util.Collection;

/**
 * Stores and retrieves secrets and associated metadata for the secrets. It must not be used to store
 * secrets/sensitive information in plain text.
 *
 */
public interface SecretStore {

  /**
   * Retrieves stored data for the secret.
   *
   * @param namespace the namespace to which secret belongs to
   * @param name the name of the secret to be retrieved
   * @return stored data
   * @throws SecretNotFoundException if secret is not found in provided namespace
   * @throws Exception if unable to retrieve the secret
   */
  byte[] get(String namespace, String name) throws Exception;

  /**
   * Provides list of all the secrets for which data is stored.
   *
   * @param namespace the namespace to which secrets belong to
   * @return list of all the secrets stored in provided namespace
   * @throws Exception if not able to get data for the secrets in the provided namespace
   */
  Collection<byte[]> list(String namespace) throws Exception;

  /**
   * Persists provided data in the store for a given secret.
   *
   * @param namespace the namespace to which secret belong to
   * @param name the name of the secret
   * @param data the data to be stored
   * @throws SecretAlreadyExistsException if the data for the secret is already stored
   * @throws Exception if unable to store the data of the secret
   */
  <T> void store(String namespace, String name, byte[] data) throws Exception;

  /**
   * Deletes the data for the provided secret.
   *
   * @param namespace the namespace to which secret belongs to
   * @param name the name of the secret to be deleted
   * @throws SecretNotFoundException if secret is not found in provided namespace
   * @throws Exception if unable to delete the data of the secret
   */
  void delete(String namespace, String name) throws Exception;
}

/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.securestore.spi;

import co.cask.cdap.securestore.spi.secret.Decoder;
import co.cask.cdap.securestore.spi.secret.Encoder;

import java.io.IOException;
import java.util.Collection;

/**
 * Stores and retrieves secrets and associated metadata for the secrets. It must not be used to store
 * secrets/sensitive information in plain text.
 *
 * The implementation of this class must be thread safe as store and retrieve methods can be called from multiple
 * threads.
 */
public interface SecretStore {

  /**
   * Retrieves stored data for the secret.
   *
   * @param namespace the namespace to which secret belongs to
   * @param name the name of the secret to be retrieved
   * @return stored data
   * @throws SecretNotFoundException if secret is not found in provided namespace
   * @throws IOException if unable to retrieve the secret
   */
  <T> T get(String namespace, String name, Decoder<T> decoder) throws SecretNotFoundException, IOException;

  /**
   * Provides list of all the secrets for which data is stored.
   *
   * @param namespace the namespace to which secrets belong to
   * @return list of all the secrets stored in provided namespace
   * @throws IOException if not able to get data for the secrets in the provided namespace
   */
  <T> Collection<T> list(String namespace, Decoder<T> decoder) throws IOException;

  /**
   * Persists provided data in the store for a given secret. If the secret already exists, it will be replaced.
   *
   * @param namespace the namespace to which secret belong to
   * @param name the name of the secret
   * @param data the data to be stored
   * @throws IOException if unable to store the data of the secret
   */
  <T> void store(String namespace, String name, Encoder<T> encoder, T data) throws IOException;

  /**
   * Deletes the data for the provided secret.
   *
   * @param namespace the namespace to which secret belongs to
   * @param name the name of the secret to be deleted
   * @throws SecretNotFoundException if secret is not found in provided namespace
   * @throws IOException if unable to delete the data of the secret
   */
  void delete(String namespace, String name) throws SecretNotFoundException, IOException;
}

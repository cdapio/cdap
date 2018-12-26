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

import co.cask.cdap.securestore.spi.secret.Secret;

import java.util.Map;

/**
 * Secrets Manager interface to store secrets securely and retrieve them when needed. Secrets are small sensitive
 * information such as passwords, database credentials, API keys etc.
 *
 * TODO CDAP-14699 Expose dataset through context in initialize method.
 */
public interface SecretsManager {

  /**
   * Returns the name of the secrets manager.
   *
   * @return non-empty name of this secrets manager
   */
  String getSecretsManagerName();

  /**
   * Initialize the secrets manager. This method is guaranteed to be called before any other method is called.
   * It will only be called once for the lifetime of the secrets manager.
   *
   * @param context the context that can be used to initialize the secrets manager
   */
  void initialize(SecretsManagerContext context) throws Exception;

  /**
   * Encrypts and stores the secret for a given namespace.
   *
   * @param namespace the namespace that this secret belongs to
   * @param name the name of the secret
   * @param secret the sensitive data that has to be securely stored
   * @param description user provided description of the secret
   * @param properties associated properties and metadata
   *
   * @throws SecretAlreadyExistsException if the secret already exists in the namespace
   * @throws Exception if unable to encrypt or store the secret
   */
  void storeSecret(String namespace, String name, byte[] secret, String description,
                   Map<String, String> properties) throws Exception;

  /**
   * Retrieves stored encrypted secret, decrypts it and returns secret in plain text along with its metadata
   * as a {@link Secret}.
   *
   * @param namespace the namespace that this secret belongs to
   * @param name the name of the secret
   * @return the sensitive data and associated metadata.
   * @throws SecretNotFoundException if the secret is not present in the namespace
   * @throws Exception if unable to retrieve or decrypt the secret
   */
  Secret getSecret(String namespace, String name) throws Exception;

  /**
   * Retrieves and returns {@link Map} of secret name and its description for the provided namespace.
   *
   * @param namespace the namespace that secrets belong to
   * @return a {@code Map} of secret name to description
   * @throws Exception if unable to retrieve secrets name or description
   */
  Map<String, String> listSecrets(String namespace) throws Exception;

  /**
   * Deletes the secret with the provided name.
   *
   * @param namespace the namespace that this secret belongs to
   * @param name the name of the secret
   * @throws SecretNotFoundException if the secret is not present in the namespace
   * @throws Exception if unable to delete the secret or associated metadata
   */
  void deleteSecret(String namespace, String name) throws Exception;
}

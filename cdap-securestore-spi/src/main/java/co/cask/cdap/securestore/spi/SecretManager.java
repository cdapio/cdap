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
import co.cask.cdap.securestore.spi.secret.SecretMetadata;

import java.util.Collection;
import java.util.Map;

/**
 * Secrets Manager interface to store secrets securely and retrieve them when needed. Secrets are small sensitive
 * information such as passwords, database credentials, API keys etc.
 *
 * TODO CDAP-14699 Expose dataset through context in initialize method.
 */
public interface SecretManager {
  /**
   * Returns the name of the secrets manager.
   *
   * @return non-empty name of this secrets manager
   */
  String getName();

  /**
   * Initialize the secrets manager. This method is guaranteed to be called before any other method is called.
   * It will only be called once for the lifetime of the secrets manager.
   *
   * @param context the context that can be used to initialize the secrets manager
   */
  void initialize(SecretManagerContext context) throws Exception;

  /**
   * Securely stores secret for a given namespace.
   *
   * @param namespace the namespace that this secret belongs to
   * @param name the name of the secret
   * @param secret the sensitive data that has to be securely stored
   * @param description user provided description of the secret
   * @param properties unmodifiable properties map representing associated properties for the secret
   * @throws SecretAlreadyExistsException if the secret already exists in the namespace
   * @throws Exception if unable to store the secret securely
   */
  void store(String namespace, String name, byte[] secret, String description,
             Map<String, String> properties) throws Exception;

  /**
   * Returns securely stored secret along with its metadata as a {@link Secret}.
   *
   * @param namespace the namespace that this secret belongs to
   * @param name the name of the secret
   * @return the sensitive data and associated metadata.
   * @throws SecretNotFoundException if the secret is not present in the` namespace
   * @throws Exception if unable to retrieve the secret
   */
  Secret get(String namespace, String name) throws Exception;

  /**
   * Returns {@link Collection} of metadata of all the secrets in the provided namespace.
   *
   * @param namespace the namespace that secrets belong to
   * @return a {@code Collection} of metadata of all the secrets in the provided namespace
   * @throws Exception if unable to list secrets
   */
  Collection<SecretMetadata> list(String namespace) throws Exception;

  /**
   * Deletes the secret with the provided name.
   *
   * @param namespace the namespace that this secret belongs to
   * @param name the name of the secret
   * @throws SecretNotFoundException if the secret is not present in the namespace
   * @throws Exception if unable to delete the secret or associated metadata
   */
  void delete(String namespace, String name) throws Exception;

  /**
   * Cleans up initialized resources. It will only be called once for the lifetime of the secrets manager.
   *
   * @param context secret manager context
   */
  void destroy(SecretManagerContext context);
}

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

/**
 * Exception that indicates a secret already exists.
 */
public class SecretAlreadyExistsException extends IllegalArgumentException {
  private final String namespace;
  private final String secretName;

  public SecretAlreadyExistsException(String namespace, String secretName) {
    super(String.format("Secret %s already exists in the namespace %s. Please provide different secret name.",
                        namespace, secretName));
    this.namespace = namespace;
    this.secretName = secretName;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getSecretName() {
    return secretName;
  }
}

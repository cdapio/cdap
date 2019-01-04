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
 * An Exception that indicates a secret is not found.
 */
public class SecretNotFoundException extends IllegalArgumentException {
  private final String namespace;
  private final String secretName;

  public SecretNotFoundException(String namespace, String secretName) {
    super(String.format("Secret %s is not found the namespace %s. Please provide correct secret name that was stored " +
                          "in %s namespace.", namespace, secretName, namespace));
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

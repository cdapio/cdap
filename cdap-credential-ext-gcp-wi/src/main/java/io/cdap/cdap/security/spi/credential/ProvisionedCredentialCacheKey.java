/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.security.spi.credential;

import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import java.util.Objects;

/**
 * Defines the contents of key used for
 * caching {@link io.cdap.cdap.proto.credential.ProvisionedCredential}.
 */
public final class ProvisionedCredentialCacheKey {
  private final NamespaceMeta namespaceMeta;
  private final CredentialIdentity credentialIdentity;
  private final String scopes;
  private transient Integer hashCode;

  /**
   * Constructs the {@link ProvisionedCredentialCacheKey}.
   *
   * @param namespaceMeta the {@link NamespaceMeta}
   * @param credentialIdentity the {@link CredentialIdentity}
   * @param scopes the comma separated list of OAuth scopes.
   */
  public ProvisionedCredentialCacheKey(NamespaceMeta namespaceMeta,
      CredentialIdentity credentialIdentity, String scopes) {
    this.namespaceMeta = namespaceMeta;
    this.credentialIdentity = credentialIdentity;
    this.scopes = scopes;
  }

  public NamespaceMeta getNamespaceMeta() {
    return namespaceMeta;
  }

  public CredentialIdentity getCredentialIdentity() {
    return credentialIdentity;
  }

  public String getScopes() {
    return scopes;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ProvisionedCredentialCacheKey)) {
      return  false;
    }
    ProvisionedCredentialCacheKey that = (ProvisionedCredentialCacheKey) o;
    return Objects.equals(namespaceMeta.getNamespaceId().getNamespace(),
        that.namespaceMeta.getNamespaceId().getNamespace())
        && Objects.equals(credentialIdentity.getSecureValue(),
        that.getCredentialIdentity().getSecureValue())
        && Objects.equals(scopes, that.scopes);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(namespaceMeta.getNamespaceId().getNamespace(),
          credentialIdentity.getSecureValue(), scopes);
    }
    return hashCode;
  }
}

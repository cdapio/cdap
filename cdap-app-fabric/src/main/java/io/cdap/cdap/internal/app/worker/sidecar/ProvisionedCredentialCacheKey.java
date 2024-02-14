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

package io.cdap.cdap.internal.app.worker.sidecar;

import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import java.util.Objects;

/**
 * Defines the contents of key used for
 * caching {@link io.cdap.cdap.proto.credential.ProvisionedCredential}.
 */
public final class ProvisionedCredentialCacheKey {
  private final GcpMetadataTaskContext gcpMetadataTaskContext;
  private final String scopes;
  private transient Integer hashCode;

  public ProvisionedCredentialCacheKey(GcpMetadataTaskContext gcpMetadataTaskContext,
      String scopes) {
    this.gcpMetadataTaskContext = gcpMetadataTaskContext;
    this.scopes = scopes;
  }

  public GcpMetadataTaskContext getGcpMetadataTaskContext() {
    return gcpMetadataTaskContext;
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

    if (gcpMetadataTaskContext == null && that.gcpMetadataTaskContext == null) {
      return Objects.equals(scopes, that.scopes);
    }

    if (gcpMetadataTaskContext == null || that.gcpMetadataTaskContext == null) {
      return false;
    }

    return Objects.equals(gcpMetadataTaskContext.getNamespace(),
        that.gcpMetadataTaskContext.getNamespace())
        && Objects.equals(gcpMetadataTaskContext.getUserCredential().toString(),
        that.gcpMetadataTaskContext.getUserCredential().toString())
        && Objects.equals(gcpMetadataTaskContext.getUserId(),
        that.gcpMetadataTaskContext.getUserId())
        && Objects.equals(gcpMetadataTaskContext.getUserIp(),
        that.gcpMetadataTaskContext.getUserIp())
        && Objects.equals(scopes, that.scopes);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {

      if (gcpMetadataTaskContext == null) {
        this.hashCode = hashCode = Objects.hash(scopes);
      } else {
        this.hashCode = hashCode = Objects.hash(gcpMetadataTaskContext.getNamespace(),
            gcpMetadataTaskContext.getUserCredential().toString(),
            gcpMetadataTaskContext.getUserId(), gcpMetadataTaskContext.getUserIp(), scopes);
      }
    }
    return hashCode;
  }
}

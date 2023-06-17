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

package io.cdap.cdap.proto.id;

import io.cdap.cdap.proto.element.EntityType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a credential provisioner identity.
 */
public class CredentialIdentityId extends NamespacedEntityId implements ParentedId<NamespaceId> {

  private final String name;
  private transient Integer hashCode;

  public CredentialIdentityId(String namespace, String name) {
    super(namespace, EntityType.CREDENTIAL_IDENTITY);
    if (name == null) {
      throw new NullPointerException("Credential identity name cannot be null.");
    }
    ensureValidId("credentialidentity", name);
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public String getEntityName() {
    return name;
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, name));
  }

  @SuppressWarnings("unused")
  public static CredentialIdentityId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new CredentialIdentityId(next(iterator, "namespace"),
        nextAndEnd(iterator, "credentialidentity"));
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    CredentialIdentityId identityId = (CredentialIdentityId) o;
    return Objects.equals(namespace, identityId.namespace)
        && Objects.equals(name, identityId.name);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, name);
    }
    return hashCode;
  }
}

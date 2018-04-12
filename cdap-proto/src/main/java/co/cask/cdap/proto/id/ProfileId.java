/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.proto.id;

import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.element.EntityType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies an artifact.
 */
public class ProfileId extends NamespacedEntityId implements ParentedId<NamespaceId> {
  public static final ProfileId DEFAULT = NamespaceId.SYSTEM.profile("default");
  private final String profileName;
  private transient Integer hashCode;

  public ProfileId(String namespace, String profileName) {
    super(namespace, EntityType.PROFILE);
    if (profileName == null) {
      throw new NullPointerException("Profile name cannot be null.");
    }
    ensureValidId("profile", profileName);
    this.profileName = profileName;
  }

  public String getProfile() {
    return profileName;
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public String getEntityName() {
    return profileName;
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, profileName));
  }

  @SuppressWarnings("unused")
  public static ProfileId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ProfileId(next(iterator, "namespace"), nextAndEnd(iterator, "profile"));
  }

  /**
   * Get the profile id for the given scoped profile name. A scoped name starts with an entity scope, a ':',
   * then the profile name. If there is no ':', it is assumed that it is user scope.
   *
   * @param namespaceId the namespace to use if user scoped
   * @param scopedName the scoped name
   * @return the profile id
   * @throws IllegalArgumentException if the scope is invalid or the profile name is invalid.
   */
  public static ProfileId fromScopedName(NamespaceId namespaceId, String scopedName) {
    int idx = scopedName.indexOf(':');
    if (idx < 0) {
      return namespaceId.profile(scopedName);
    }
    if (idx == scopedName.length()) {
      throw new IllegalArgumentException("Invalid scoped profile " + scopedName + ". Cannot end with a ':'.");
    }
    EntityScope scope = EntityScope.valueOf(scopedName.substring(0, idx).toUpperCase());
    String profileName = scopedName.substring(idx + 1);
    return scope == EntityScope.SYSTEM ? NamespaceId.SYSTEM.profile(profileName) : namespaceId.profile(profileName);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ProfileId profileId = (ProfileId) o;
    return Objects.equals(namespace, profileId.namespace) &&
      Objects.equals(profileName, profileId.profileName);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, profileName);
    }
    return hashCode;
  }
}

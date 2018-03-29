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

import co.cask.cdap.proto.element.EntityType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies an artifact.
 */
public class ProfileId extends NamespacedEntityId implements ParentedId<NamespaceId> {
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

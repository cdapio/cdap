/*
 * Copyright © 2017 Cask Data, Inc.
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

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * <p>
 * Represents a Kerberos Principal and also extends {@link EntityId} to support granting
 * {@link io.cdap.cdap.proto.security.Privilege} and other Authorization operations.
 * </p>
 * <p>
 * Note: This class should not be confused with {@link io.cdap.cdap.proto.security.Principal} which represents a
 * user, group or role in CDAP to whom {@link io.cdap.cdap.proto.security.Privilege} can be given.
 * Whereas this {@link KerberosPrincipalId} class represent a Kerberos principal on
 * which {@link io.cdap.cdap.proto.security.Permission} can be granted to {@link io.cdap.cdap.proto.security.Principal}
 * to represent a {@link io.cdap.cdap.proto.security.GrantedPermission}.
 * </p>
 * <p>
 * For example, if a {@link io.cdap.cdap.proto.security.Principal} has
 * {@link io.cdap.cdap.proto.security.StandardPermission#GET} on a {@link KerberosPrincipalId} it signifies that the
 * {@link io.cdap.cdap.proto.security.Principal} can READ (use) the {@link KerberosPrincipalId} to impersonate the user
 * of the {@link KerberosPrincipalId}.
 * </p>
 * <p>
 * Usually user operating kerberos principal would need one of {@link io.cdap.cdap.proto.security.AccessPermission} on
 * that principal.
 * </p>
 * <p>
 * This class does not perform any kind of validation while creating an instance through
 * {@link KerberosPrincipalId#KerberosPrincipalId(String)} to check if the principal is valid format or not.
 * Its the responsibility of the client to perform validation if needed.
 * </p>
 */
public class KerberosPrincipalId extends EntityId {

  private final String principal;
  private transient Integer hashCode;

  public KerberosPrincipalId(String principal) {
    super(EntityType.KERBEROSPRINCIPAL);
    if (principal == null || principal.isEmpty()) {
      throw new NullPointerException("Principal cannot be null or empty");
    }
    this.principal = principal;
  }

  public String getPrincipal() {
    return principal;
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.singletonList(principal);
  }

  @Override
  public String getEntityName() {
    return getPrincipal();
  }

  @SuppressWarnings("unused")
  public static KerberosPrincipalId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new KerberosPrincipalId(nextAndEnd(iterator, "principal"));
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    KerberosPrincipalId other = (KerberosPrincipalId) o;
    return Objects.equals(principal, other.principal);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), principal);
    }
    return hashCode;
  }
}

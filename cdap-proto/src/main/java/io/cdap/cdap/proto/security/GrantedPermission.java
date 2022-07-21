/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.proto.security;

import io.cdap.cdap.proto.id.EntityId;

import java.util.Objects;
/**
 * Represents a permission granted to a {@link Principal user}, {@link Principal group} or a {@link Principal role}.
 * It determines if the user or group can perform a given action on an {@link EntityId}.
 */
public class GrantedPermission {
  private final Authorizable authorizable;
  private final Permission permission;

  public GrantedPermission(EntityId entityId, Permission permission) {
    this(Authorizable.fromEntityId(entityId), permission);
  }

  public GrantedPermission(Authorizable authorizable, Permission permission) {
    this.authorizable = authorizable;
    this.permission = permission;
  }

  public Authorizable getAuthorizable() {
    return authorizable;
  }

  public Permission getPermission() {
    return permission;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GrantedPermission)) {
      return false;
    }

    GrantedPermission privilege = (GrantedPermission) o;
    return Objects.equals(authorizable, privilege.authorizable) && Objects.equals(permission, privilege.permission);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authorizable, permission);
  }

  @Override
  public String toString() {
    return "GrantedPermission {" +
      "authorizable=" + authorizable +
      ", permission=" + permission +
      '}';
  }}

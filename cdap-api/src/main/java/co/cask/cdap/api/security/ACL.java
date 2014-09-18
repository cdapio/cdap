/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.security;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Represents an entry in the Access Control List.
 */
public class ACL {

  private final Principal principal;
  private final Set<PermissionType> permissions;

  public ACL(Principal principal, Iterable<PermissionType> permissions) {
    this.principal = principal;
    this.permissions = ImmutableSet.copyOf(permissions);
  }

  public Principal getPrincipal() {
    return principal;
  }

  public Set<PermissionType> getPermissions() {
    return permissions;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ACL{");
    sb.append("principal=").append(principal);
    sb.append(", permissions=").append(permissions);
    sb.append('}');
    return sb.toString();
  }
}

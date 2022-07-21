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

/**
 * This enum holds a list of all know {@link Permission} enums and can be used to help identify specific permission.
 */
public enum PermissionType {
  STANDARD(StandardPermission.class),
  APPLICATION(ApplicationPermission.class),
  ACCESS(AccessPermission.class),
  INSTANCE(InstancePermission.class);

  private final Class<? extends Permission> permissionClass;

  PermissionType(Class<? extends Permission> permissionClass) {
    this.permissionClass = permissionClass;
  }

  /**
   *
   * @return enum class for this permission type. All permissions of that class must return this type from their
   * {@link Permission#getPermissionType()} call.
   */
  public Class<? extends Permission> getPermissionClass() {
    return permissionClass;
  }

  /**
   *
   * @param permissionType {@link #name} of {@link Permission#getPermissionType()}
   * @param permissionName {@link Permission#name()}
   * @return {@link Permission} identified by type and name pair
   * @throws IllegalArgumentException if type or name is not known
   */
  public static Permission valueOf(String permissionType, String permissionName) {
    PermissionType tp = valueOf(permissionType);
    Class permissionClass = tp.getPermissionClass();
    return (Permission) Enum.valueOf(permissionClass, permissionName);
  }
}

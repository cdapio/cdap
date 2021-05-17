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
 * Permissions that {@link Principal} must be granted to perform various actions on
 * {@link io.cdap.cdap.proto.id.EntityId}. Each class of permissions is represented by an enum.
 * {@link PermissionType} holds a list of all known permission types.
 */
public interface Permission extends ActionOrPermission {
  /**
   *
   * @return if specific permission can be checked on parent. It's usually used for permission checked when
   * entity have not got an id yet, e.g. "create" permission.
   */
  default boolean isCheckedOnParent() {
    return false;
  }

  /**
   *
   * @return type this permission belongs to. Permission type and name can be used to identify it using
   * {@link PermissionType#valueOf(String, String)}
   */
  PermissionType getPermissionType();

  /**
   *
   * @return permission name. Usually it's just delegated to {@link Enum#name()}
   */
  String name();
}

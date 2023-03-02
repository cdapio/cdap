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
 * Set of standard CRUD permissions to manipulate entities.
 */
public enum StandardPermission implements Permission {
  /**
   * Create an entity. Can be used both with enforceOnParent when name is not known beforehand and
   * with enforce when name is provided.
   */
  CREATE {
    @Override
    public boolean isCheckedOnParent() {
      return true;
    }
  },
  /**
   * List permission can be used on parent entity to check if a principal has access to list
   * specific children types.
   */
  LIST {
    @Override
    public boolean isCheckedOnParent() {
      return true;
    }
  },
  /**
   * Read access to an entity.
   */
  GET,
  /**
   * Update access to an entity.
   */
  UPDATE,
  /**
   * Permission to delete specific entity.
   */
  DELETE,
  /**
   * Permission to use specific entity.
   */
  USE;

  @Override
  public PermissionType getPermissionType() {
    return PermissionType.STANDARD;
  }
}

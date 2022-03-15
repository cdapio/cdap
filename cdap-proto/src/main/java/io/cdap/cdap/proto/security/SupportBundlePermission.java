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
 * Set of supportBundle permissions to manipulate entities.
 */
public enum SupportBundlePermission implements Permission {
  /**
   * Create an entity. Can be used both with enforceOnParent when name is not known beforehand and with
   * enforce when name is provided.
   */
  GENERATE_SUPPORT_BUNDLE {
    @Override
    public boolean isCheckedOnParent() {
      return true;
    }
  };

  @Override
  public PermissionType getPermissionType() {
    return PermissionType.STANDARD;
  }
}

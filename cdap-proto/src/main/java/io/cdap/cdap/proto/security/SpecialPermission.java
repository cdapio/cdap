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

public enum SpecialPermission implements Permission {
  /**
   * This permission should only be used for backwards compatibility
   * when there is a need to deserialize a permission that authentication extension do not know. Usually it should
   * be handled similar to {@link StandardPermission#UPDATE}
   */
  OTHER,
  /**
   * This permission should only be used for backwards compatibility
   * when there is a need to deserialize a permission that authentication extension do not know. Unlike {@link #OTHER}
   * this permission replaces any unknown permission that is checked on parent.
   * Usually it should be handled similar to {@link StandardPermission#CREATE}
   */
  OTHER_CHECKED_ON_PARENT {
    @Override
    public boolean isCheckedOnParent() {
      return true;
    }
  },
  ;

  @Override
  public PermissionType getPermissionType() {
    return PermissionType.SPECIAL;
  }
}

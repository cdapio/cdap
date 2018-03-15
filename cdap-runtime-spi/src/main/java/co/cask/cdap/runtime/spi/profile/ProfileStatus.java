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

package co.cask.cdap.runtime.spi.profile;

/**
 * The status of the profile.
 */
public enum ProfileStatus {
  /**
   * If the status of a profile is enabled, it can be assigned to any program. A profile cannot be deleted
   * when its status is enabled.
   */
  ENABLED,

  /**
   * If the status of a profile is disabled, it cannot be assigned to any program. A profile can only be deleted
   * when its status is disabled.
   */
  DISABLED
}

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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.DatasetManagementException;

/**
 * Thrown when operation conflicts with existing {@link co.cask.cdap.api.dataset.module.DatasetModule}s
 * in the system.
 */
public class ModuleConflictException extends DatasetManagementException {
  public ModuleConflictException(String message) {
    super(message);
  }

  public ModuleConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}

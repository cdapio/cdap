/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api.dataset;

/**
 * Thrown for an operation on a non-existing dataset instance.
 */
public class InstanceNotFoundException extends DatasetManagementException {
  public InstanceNotFoundException(String instanceName) {
    super(generateMessage(instanceName));
  }

  public InstanceNotFoundException(String instanceName, Throwable cause) {
    super(generateMessage(instanceName), cause);
  }

  private static String generateMessage(String instanceName) {
    return String.format("Dataset instance '%s' does not exist", instanceName);
  }

}

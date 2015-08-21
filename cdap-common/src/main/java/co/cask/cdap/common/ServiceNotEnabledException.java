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

package co.cask.cdap.common;

/**
 * Thrown when the user tried to execute some service operation, but the service was not enabled.
 */

public class ServiceNotEnabledException extends Exception {

  private final String serviceName;

  public ServiceNotEnabledException(String serviceName) {
    super("Service '" + serviceName + "' was not enabled");
    this.serviceName = serviceName;
  }

  public String getServiceName() {
    return serviceName;
  }
}

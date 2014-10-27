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

package co.cask.cdap.api.service.http;

import co.cask.cdap.api.service.Service;
import com.google.common.base.Objects;

/**
 * Describes an endpoint that a {@link Service} exposes.
 */
public final class ServiceHttpEndpoint {

  private final String method;
  private final String path;

  /**
   * Create an instance of {@link ServiceHttpEndpoint}.
   * @param method type of method.
   * @param path path of the endpoint.
   */
  public ServiceHttpEndpoint(String method, String path) {
    this.method = method;
    this.path = path;
  }

  public String getMethod() {
    return method;
  }

  public String getPath() {
    return path;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(method, path);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ServiceHttpEndpoint other = (ServiceHttpEndpoint) obj;
    return Objects.equal(this.method, other.method) && Objects.equal(this.path, other.path);
  }
}

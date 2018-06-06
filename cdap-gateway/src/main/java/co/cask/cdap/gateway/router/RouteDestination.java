/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
package co.cask.cdap.gateway.router;

import java.util.Objects;
import javax.annotation.Nullable;

/**
* Class to identify a routing destination.
*/
final class RouteDestination {
  private final String serviceName;
  private final String version;
  private final int hashCode;

  RouteDestination(String serviceName) {
    this(serviceName, null);
  }

  RouteDestination(String serviceName, @Nullable String payload) {
    this.serviceName = serviceName;
    this.version = payload;
    this.hashCode = Objects.hash(serviceName, payload);
  }

  String getServiceName() {
    return serviceName;
  }

  @Nullable
  public String getVersion() {
    return version;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RouteDestination other = (RouteDestination) o;
    return Objects.equals(serviceName, other.getServiceName()) && Objects.equals(version, other.getVersion());
  }

  @Override
  public String toString() {
    return "{serviceName=" + serviceName + ", version=" + version + "}";
  }
}

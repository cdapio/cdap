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

package co.cask.cdap.api;

import java.net.URL;

/**
 * An interface for Discovery Service.
 */
public interface ServiceDiscoverer {

  /**
   * Discover the base URL for a Service, relative to which Service endpoints can be accessed
   *
   * @param applicationId Application name
   * @param serviceId     Service name
   * @return URL for the discovered service or null if the service is not found
   */
  URL getServiceURL(String applicationId, String serviceId);

  /**
   * Discover the base URL for a Service in the same application, relative to which Service endpoints can be accessed
   *
   * @param serviceId Service Name
   * @return URL for the discovered service or null if the service is not found
   */
  URL getServiceURL(String serviceId);
}

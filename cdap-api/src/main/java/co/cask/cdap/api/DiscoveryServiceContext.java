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
public interface DiscoveryServiceContext {

  /**
   * Used to discover services inside a given application.
   *
   * @param applicationId Application name
   * @param serviceId     Service name
   * @return URL
   */
  URL getServiceURL(String applicationId, String serviceId);

  /**
   * Omitting an applicationId assumes that the program wants to discover a service within its own application.
   *
   * @param serviceId Service Name
   * @return URL
   */
  URL getServiceURL(String serviceId);
}

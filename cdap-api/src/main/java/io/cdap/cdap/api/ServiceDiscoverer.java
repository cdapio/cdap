/*
 * Copyright © 2014-2021 Cask Data, Inc.
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

package io.cdap.cdap.api;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * An interface for Discovery Service.
 */
public interface ServiceDiscoverer {

  /**
   * Discover the base URL for a Service of an Application running in a Namespace, relative to which Service endpoints
   * can be accessed
   *
   * @param namespaceId   Namespace of the application
   * @param applicationId Application name
   * @param serviceId     Service name
   * @return URL for the discovered service or null if the service is not found
   */
  @Nullable
  default URL getServiceURL(String namespaceId, String applicationId, String serviceId) {
    throw new UnsupportedOperationException("NamespaceId service discovery is not supported.");
  }

  /**
   * Discover the base URL for a Service, relative to which Service endpoints can be accessed
   *
   * @param applicationId Application name
   * @param serviceId     Service name
   * @return URL for the discovered service or null if the service is not found
   */
  @Nullable
  URL getServiceURL(String applicationId, String serviceId);

  /**
   * Discover the base URL for a Service in the same application, relative to which Service endpoints can be accessed
   *
   * @param serviceId Service Name
   * @return URL for the discovered service or null if the service is not found
   */
  @Nullable
  URL getServiceURL(String serviceId);

  /**
   * Opens a {@link HttpURLConnection} for connecting to the given Service endpoint.
   *
   * @param namespaceId Namespace of the application
   * @param applicationId Application name
   * @param serviceId Service name
   * @param methodPath Service method path as declared by the service handler
   * @return a {@link HttpURLConnection} for communicating with the Service endpoint, or {@code null} if the service
   *         is not found
   * @throws IOException if Service is found but failed to open a connection to the given Service endpoint
   */
  @Nullable
  default HttpURLConnection openConnection(String namespaceId, String applicationId,
                                           String serviceId, String methodPath) throws IOException {
    throw new UnsupportedOperationException("Connection is not supported");
  }
}

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

package io.cdap.cdap.app.services;

import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * An abstract implementation of {@link ServiceDiscoverer}.
 * It provides definition for {@link ServiceDiscoverer#getServiceURL}  and expects the sub-classes to give definition
 * for {@link AbstractServiceDiscoverer#getDiscoveryServiceClient}.
 */
public abstract class AbstractServiceDiscoverer implements ServiceDiscoverer {

  private final String namespaceId;
  private final String applicationId;

  public AbstractServiceDiscoverer(ProgramId programId) {
    this.namespaceId = programId.getNamespace();
    this.applicationId = programId.getApplication();
  }

  @Override
  public URL getServiceURL(String namespaceId, String applicationId, String serviceId) {
    try {
      return createRemoteClient(namespaceId, applicationId, serviceId).resolve("");
    } catch (ServiceUnavailableException e) {
      return null;
    }
  }

  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    return getServiceURL(namespaceId, applicationId, serviceId);
  }

  @Override
  public URL getServiceURL(String serviceId) {
    return getServiceURL(applicationId, serviceId);
  }

  @Nullable
  @Override
  public HttpURLConnection openConnection(String namespaceId, String applicationId,
                                          String serviceId, String methodPath) throws IOException {
    try {
      return createRemoteClient(namespaceId, applicationId, serviceId).openConnection(methodPath);
    } catch (ServiceUnavailableException e) {
      return null;
    }
  }

  /**
   * @return the {@link DiscoveryServiceClient} for Service Discovery
   */
  protected abstract DiscoveryServiceClient getDiscoveryServiceClient();

  /**
   * Creates a {@link RemoteClient} for connecting to the given service endpoint.
   *
   * @param namespaceId namespace of the service
   * @param applicationId application of the service
   * @param serviceId service name of the service
   * @return a {@link RemoteClient} that is setup to be used in the current execution environment.
   */
  private RemoteClient createRemoteClient(String namespaceId, String applicationId, String serviceId) {
    String discoveryName = ServiceDiscoverable.getName(namespaceId, applicationId, ProgramType.SERVICE, serviceId);
    String basePath = String.format("%s/namespaces/%s/apps/%s/services/%s/methods/",
                                    Constants.Gateway.API_VERSION_3_TOKEN, namespaceId, applicationId, serviceId);
    return new RemoteClient(getDiscoveryServiceClient(), discoveryName, HttpRequestConfig.DEFAULT, basePath);
  }
}

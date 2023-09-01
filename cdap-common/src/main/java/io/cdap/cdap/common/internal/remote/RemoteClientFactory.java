/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.InternalRouter;
import io.cdap.cdap.common.conf.Constants.Service;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory to create {@link RemoteClient}.
 */
public class RemoteClientFactory {

  public static final HttpRequestConfig NO_VERIFY_HTTP_REQUEST_CONFIG = new HttpRequestConfig(
      15000,
      15000,
      false);
  private static final Logger LOG = LoggerFactory.getLogger(
      RemoteClientFactory.class);
  private final DiscoveryServiceClient discoveryClient;
  private final InternalAuthenticator internalAuthenticator;
  private final RemoteAuthenticator remoteAuthenticator;
  private final String pathPrefix;
  private final boolean internalRouterEnabled;

  @VisibleForTesting
  public RemoteClientFactory(DiscoveryServiceClient discoveryClient,
      InternalAuthenticator internalAuthenticator) {
    this(discoveryClient, internalAuthenticator, new NoOpRemoteAuthenticator(),
        "", false);
  }

  @Inject
  public RemoteClientFactory(DiscoveryServiceClient discoveryClient,
      InternalAuthenticator internalAuthenticator,
      RemoteAuthenticator remoteAuthenticator, CConfiguration cConf) {
    this(discoveryClient, internalAuthenticator, remoteAuthenticator, "",
        cConf.getBoolean(InternalRouter.INTERNAL_ROUTER_ENABLED));
  }

  public RemoteClientFactory(DiscoveryServiceClient discoveryClient,
      InternalAuthenticator internalAuthenticator,
      RemoteAuthenticator remoteAuthenticator) {
    this(discoveryClient, internalAuthenticator, remoteAuthenticator, "",
        false);
  }


  public RemoteClientFactory(DiscoveryServiceClient discoveryClient,
      InternalAuthenticator internalAuthenticator,
      RemoteAuthenticator remoteAuthenticator, String pathPrefix) {
    this(discoveryClient, internalAuthenticator, remoteAuthenticator,
        pathPrefix, false);
  }

  /**
   * Constructs a {@link RemoteClientFactory} for creating {@link RemoteClient}s
   * for CDAP services.
   *
   * @param discoveryClient       Discovery client for getting discoverables.
   * @param pathPrefix            URL prefix to be added to the base path of
   *                              each client.
   * @param internalRouterEnabled Boolean indicating whether the
   *                              {@link InternalRouter} service for routing. If
   *                              true, clients to the Internal Router service
   *                              will be created and requests will be routed to
   *                              destination services via the router.
   */
  public RemoteClientFactory(DiscoveryServiceClient discoveryClient,
      InternalAuthenticator internalAuthenticator,
      RemoteAuthenticator remoteAuthenticator, String pathPrefix,
      boolean internalRouterEnabled) {
    this.discoveryClient = discoveryClient;
    this.internalAuthenticator = internalAuthenticator;
    this.remoteAuthenticator = remoteAuthenticator;
    this.pathPrefix = pathPrefix;
    this.internalRouterEnabled = internalRouterEnabled;
  }

  /**
   * Creates a {@link RemoteClient}.
   *
   * @param discoverableServiceName Name of the CDAP service for which a client
   *                                is requested.
   * @param basePath                Common path prefix for all the requests that
   *                                will be sent using the returned client.
   * @return A {@link RemoteClient} for the requested service.
   */
  public RemoteClient createRemoteClient(String discoverableServiceName,
      HttpRequestConfig httpRequestConfig, String basePath) {
    basePath = basePath.startsWith("/") ? pathPrefix + basePath
        : pathPrefix + "/" + basePath;
    if (this.internalRouterEnabled) {
      return getClientForInternalRouter(discoverableServiceName,
          httpRequestConfig, basePath);
    }
    return new RemoteClient(internalAuthenticator, discoveryClient,
        discoverableServiceName, httpRequestConfig, basePath,
        remoteAuthenticator);
  }

  /**
   * Creates a {@link RemoteClient}.
   *
   * @param discoverableServiceName Name of the CDAP service for which a client
   *                                is requested.
   * @param httpRequestConfig       The {@link HttpRequestConfig}.
   * @param basePath                Common path prefix for all the requests that
   *                                will be sent using the returned client.
   * @param internalAuthenticator   The {@link InternalAuthenticator}.
   * @return A {@link RemoteClient} for the requested service.
   */
  public RemoteClient createRemoteClient(String discoverableServiceName,
      HttpRequestConfig httpRequestConfig, String basePath,
      InternalAuthenticator internalAuthenticator) {
    basePath = basePath.startsWith("/") ? pathPrefix + basePath : pathPrefix + "/" + basePath;
    return new RemoteClient(internalAuthenticator, discoveryClient, discoverableServiceName,
        httpRequestConfig, basePath, remoteAuthenticator);
  }

  private RemoteClient getClientForInternalRouter(String destinationServiceName,
      HttpRequestConfig httpRequestConfig, String basePath) {
    LOG.trace(
        "Creating client for service '{}' which routes through service '{}'.",
        destinationServiceName, Service.INTERNAL_ROUTER);
    String internalRouterPath = String.format("/%s/router/services/%s%s",
        Constants.Gateway.INTERNAL_API_VERSION_3, destinationServiceName,
        basePath);
    return new RemoteClient(internalAuthenticator, discoveryClient,
        Service.INTERNAL_ROUTER, httpRequestConfig, internalRouterPath,
        remoteAuthenticator);
  }
}

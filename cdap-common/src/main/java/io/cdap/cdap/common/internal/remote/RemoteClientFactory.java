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
import io.cdap.cdap.common.conf.Constants.Service;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.common.http.HttpRequestConfig;
import javax.inject.Named;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * A factory to create {@link RemoteClient}.
 */
public class RemoteClientFactory {

  public static final HttpRequestConfig NO_VERIFY_HTTP_REQUEST_CONFIG = new HttpRequestConfig(15000,
      15000,
      false);
  public static final String USE_INTERNAL_ROUTER = "useInternalRouter";
  private final DiscoveryServiceClient discoveryClient;
  private final InternalAuthenticator internalAuthenticator;
  private final RemoteAuthenticator remoteAuthenticator;
  private final String pathPrefix;
  private boolean useInternalRouter = false;

  @VisibleForTesting
  public RemoteClientFactory(DiscoveryServiceClient discoveryClient,
      InternalAuthenticator internalAuthenticator) {
    this(discoveryClient, internalAuthenticator, new NoOpRemoteAuthenticator(), "");
  }

  @Inject
  public RemoteClientFactory(DiscoveryServiceClient discoveryClient,
      InternalAuthenticator internalAuthenticator,
      RemoteAuthenticator remoteAuthenticator) {
    this(discoveryClient, internalAuthenticator, remoteAuthenticator, "");
  }

  @Inject(optional=true)
  public void setUseInternalRouter(@Named(USE_INTERNAL_ROUTER) boolean useInternalRouter) {
    this.useInternalRouter = useInternalRouter;
  }

  public RemoteClientFactory(DiscoveryServiceClient discoveryClient,
      InternalAuthenticator internalAuthenticator,
      RemoteAuthenticator remoteAuthenticator, String pathPrefix) {
    this.discoveryClient = discoveryClient;
    this.internalAuthenticator = internalAuthenticator;
    this.remoteAuthenticator = remoteAuthenticator;
    this.pathPrefix = pathPrefix;
  }

  public RemoteClient createRemoteClient(String discoverableServiceName,
      HttpRequestConfig httpRequestConfig,
      String basePath) {
    basePath = basePath.startsWith("/") ? pathPrefix + basePath : pathPrefix + "/" + basePath;
    String serviceToDiscover;
    String finalBasePath;
    if (useInternalRouter) {
      System.out.println("Routing service through the internal router: " + discoverableServiceName);
      serviceToDiscover = Service.INTERNAL_ROUTER;
      finalBasePath = String.format("/v3Internal/router/services/%s%s",
          discoverableServiceName, basePath);
    } else {
      finalBasePath = basePath;
      serviceToDiscover = discoverableServiceName;
    }
    return new RemoteClient(internalAuthenticator, discoveryClient, serviceToDiscover,
        httpRequestConfig, finalBasePath, remoteAuthenticator);
  }
}

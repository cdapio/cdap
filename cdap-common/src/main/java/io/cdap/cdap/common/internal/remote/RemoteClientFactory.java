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
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.twill.discovery.DiscoveryServiceClient;

import javax.inject.Inject;

/**
 * A factory to create {@link RemoteClient}.
 */
public class RemoteClientFactory {
  public static final HttpRequestConfig NO_VERIFY_HTTP_REQUEST_CONFIG = new HttpRequestConfig(15000,
                                                                                              15000,
                                                                                              false);
  private final DiscoveryServiceClient discoveryClient;
  private final InternalAuthenticator internalAuthenticator;
  private final RemoteAuthenticator remoteAuthenticator;

  @VisibleForTesting
  public RemoteClientFactory(DiscoveryServiceClient discoveryClient, InternalAuthenticator internalAuthenticator) {
    this(discoveryClient, internalAuthenticator, new NoOpRemoteAuthenticator());
  }

  @Inject
  public RemoteClientFactory(DiscoveryServiceClient discoveryClient, InternalAuthenticator internalAuthenticator,
                             RemoteAuthenticator remoteAuthenticator) {
    this.discoveryClient = discoveryClient;
    this.internalAuthenticator = internalAuthenticator;
    this.remoteAuthenticator = remoteAuthenticator;
  }

  public RemoteClient createRemoteClient(String discoverableServiceName, HttpRequestConfig httpRequestConfig,
                                         String basePath) {
    return createRemoteClient(discoverableServiceName, httpRequestConfig, basePath, false);
  }

  public RemoteClient createRemoteClient(String discoverableServiceName, HttpRequestConfig httpRequestConfig,
                                         String basePath, boolean skipRewriterUrl) {
    return new RemoteClient(internalAuthenticator, discoveryClient, discoverableServiceName,
                            httpRequestConfig, basePath, remoteAuthenticator, skipRewriterUrl);
  }
}

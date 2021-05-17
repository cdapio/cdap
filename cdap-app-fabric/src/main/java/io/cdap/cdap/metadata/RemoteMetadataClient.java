/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.metadata.AbstractMetadataClient;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

/**
 * Metadata client which should be used in program container to make request to metadata service which is discovered
 * through {@link DiscoveryServiceClient}.
 */
public class RemoteMetadataClient extends AbstractMetadataClient {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteMetadataClient.class);

  private final RemoteClient remoteClient;
  private final AuthenticationContext authenticationContext;

  @Inject
  RemoteMetadataClient(final DiscoveryServiceClient discoveryClient,
                       AuthenticationContext authenticationContext) {
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.METADATA_SERVICE,
                                         new DefaultHttpRequestConfig(false), Constants.Gateway.API_VERSION_3);
    this.authenticationContext = authenticationContext;
  }

  @Override
  protected HttpResponse execute(HttpRequest request, int... allowedErrorCodes) throws IOException {
    LOG.trace("Making metadata request {}", request);
    HttpResponse response = remoteClient.execute(addUserIdHeader(request));
    LOG.trace("Received response {} for request {}", response, request);
    return response;
  }

  @Override
  protected URL resolve(NamespaceId namespace, String resource) {
    // this is only required for search which we don't support from program containers as of now
    throw new UnsupportedOperationException("Namespaced operations is not supported from programs");
  }

  @Override
  protected URL resolve(String resource) {
    URL url = remoteClient.resolve(resource);
    LOG.trace("Resolved URL {} for resources {}", url, resource);
    return url;
  }

  private HttpRequest addUserIdHeader(HttpRequest request) {
    return new HttpRequest.Builder(request).addHeader(Constants.Security.Headers.USER_ID,
                                                      authenticationContext.getPrincipal().getName()).build();
  }
}

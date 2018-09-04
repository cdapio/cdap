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

package co.cask.cdap.metadata;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.common.metadata.AbstractMetadataClient;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.inject.Inject;
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
  protected HttpResponse execute(HttpRequest request, int... allowedErrorCodes) throws IOException,
    UnauthorizedException {
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

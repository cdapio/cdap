/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.program;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.program.AbstractProgramClient;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

/**
 * Program client to make requests to the App Fabric service which is discovered through {@link
 * DiscoveryServiceClient}.
 */
public class RemoteProgramClient extends AbstractProgramClient {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteProgramClient.class);

  private final RemoteClient remoteClient;

  @Inject
  public RemoteProgramClient(DiscoveryServiceClient discoveryClient) {
    HttpRequestConfig httpRequestConfig = new DefaultHttpRequestConfig(false);
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.APP_FABRIC_HTTP, httpRequestConfig,
                                         Constants.Gateway.API_VERSION_3);
  }

  @Override
  protected HttpResponse execute(HttpRequest request, int... allowedErrorCodes) throws IOException,
    UnauthorizedException {
    LOG.trace("Making program request {}", request.getBody());
    HttpResponse response = remoteClient.execute(request);
    LOG.trace("Received response {} for request {}", response, request);
    return response;
  }

  @Override
  protected URL resolve(String resource) {
    URL url = remoteClient.resolve(resource);
    LOG.trace("Resolved URL {} for resources {}", url, resource);
    return url;
  }

}

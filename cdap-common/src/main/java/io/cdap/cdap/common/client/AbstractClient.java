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

package io.cdap.cdap.common.client;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.URL;

/**
 * Abstract client to interact with app fabric service over HTTP.
 */
public abstract class AbstractClient {

  private final RemoteClient remoteClient;

  public AbstractClient(DiscoveryServiceClient discoveryClient) {
    HttpRequestConfig httpRequestConfig = new DefaultHttpRequestConfig(false);
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.APP_FABRIC_HTTP, httpRequestConfig,
                                         Constants.Gateway.API_VERSION_3);
  }

  /**
   * Executes an HTTP request.
   */
  protected HttpResponse execute(HttpRequest request, int... allowedErrorCodes)
    throws IOException, UnauthorizedException {
    return remoteClient.execute(request);
  }

  /**
   * Resolved the specified URL
   */
  protected URL resolve(String resource) {
    return remoteClient.resolve(resource);
  }

}

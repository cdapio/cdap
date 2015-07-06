/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import javax.inject.Inject;

/**
 * Client to interact with CDAP namespaces
 */
public class NamespaceClient extends AbstractNamespaceClient {
  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public NamespaceClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public NamespaceClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  @Override
  protected HttpResponse execute(HttpRequest request) throws IOException, UnauthorizedException {
    // the allowed codes are the ones that AbstractNamespaceClient expects to be able to handle
    return restClient.execute(request, config.getAccessToken(),
                              HttpURLConnection.HTTP_BAD_REQUEST,
                              HttpURLConnection.HTTP_NOT_FOUND,
                              HttpURLConnection.HTTP_FORBIDDEN);
  }

  @Override
  protected URL resolve(String resource) throws MalformedURLException {
    return config.resolveURLV3(resource);
  }
}

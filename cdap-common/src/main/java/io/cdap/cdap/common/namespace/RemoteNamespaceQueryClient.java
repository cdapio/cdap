/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.common.namespace;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.HttpHandler;

import java.io.IOException;
import java.net.URL;

/**
 * Implementation of {@link NamespaceQueryAdmin} that pings the internal endpoints in a {@link HttpHandler} in remote
 * system service.
 */
public class RemoteNamespaceQueryClient extends AbstractNamespaceQueryClient {
  private final RemoteClient remoteClient;

  @Inject
  RemoteNamespaceQueryClient(RemoteClientFactory remoteClientFactory) {
    this.remoteClient = remoteClientFactory.createRemoteClient(
      Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false), Constants.Gateway.API_VERSION_3);
  }

  @Override
  protected HttpResponse execute(HttpRequest request) throws IOException, UnauthorizedException {
    return remoteClient.execute(request);
  }

  @Override
  protected URL resolve(String resource) throws IOException {
    return remoteClient.resolve(resource);
  }
}


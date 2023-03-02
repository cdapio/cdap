/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.healthcheck;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * Fetch health check via internal REST API calls
 */
public class VMInformationFetcher {

  private static final Gson GSON = new Gson();

  private final RemoteClientFactory remoteClientFactory;

  @Inject
  VMInformationFetcher(RemoteClientFactory remoteClientFactory) {
    this.remoteClientFactory = remoteClientFactory;
  }

  /**
   * Get the application detail for the given application id
   */
  public VMInformation getVMInformation(String serviceName) throws NotFoundException, IOException {
    RemoteClient remoteClient = remoteClientFactory.createRemoteClient(serviceName,
        new DefaultHttpRequestConfig(false),
        Constants.Gateway.API_VERSION_3);

    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, "vminfo");
    HttpResponse httpResponse = execute(remoteClient, requestBuilder.build());
    return GSON.fromJson(httpResponse.getResponseBodyAsString(), VMInformation.class);
  }

  private HttpResponse execute(RemoteClient remoteClient, HttpRequest request)
      throws IOException, NotFoundException {
    HttpResponse httpResponse = remoteClient.execute(request);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(httpResponse.getResponseBodyAsString());
    }
    return httpResponse;
  }
}

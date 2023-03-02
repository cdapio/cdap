/*
 *  Copyright © 2022 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.master.environment.k8s;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;

/**
 * Creates Kubernetes ApiClients.
 */
public class DefaultApiClientFactory implements ApiClientFactory {

  private final long connectTimeoutSec;
  private final long readTimeoutSec;

  public DefaultApiClientFactory(long connectTimeoutSec, long readTimeoutSec) {
    this.connectTimeoutSec = connectTimeoutSec;
    this.readTimeoutSec = readTimeoutSec;
  }

  public ApiClient create() throws IOException {
    ApiClient client = Config.defaultClient();
    OkHttpClient httpClient = client.getHttpClient().newBuilder()
        .connectTimeout(connectTimeoutSec, TimeUnit.SECONDS)
        .readTimeout(readTimeoutSec, TimeUnit.SECONDS)
        .build();
    client.setHttpClient(httpClient);
    return client;
  }

}

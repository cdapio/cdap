/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.client;

import com.continuuity.client.config.ReactorClientConfig;
import com.continuuity.client.util.RESTClient;
import com.continuuity.common.http.HttpMethod;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.common.http.ObjectResponse;
import com.continuuity.proto.ServiceMeta;

import java.io.IOException;
import java.net.URL;
import javax.inject.Inject;

/**
 * Provides ways to interact with Reactor User Services.
 */
public class ServiceClient {

  private final RESTClient restClient;
  private final ReactorClientConfig config;

  @Inject
  public ServiceClient(ReactorClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(config);
  }

  /**
   * Gets information about a service.
   * 
   * @param appId ID of the application that the service belongs to
   * @param serviceId ID of the service
   * @return {@link ServiceMeta} representing the service.
   * @throws IOException if a network error occurred
   */
  public ServiceMeta get(String appId, String serviceId) throws IOException {
    URL url = config.resolveURL(String.format("apps/%s/services/%s", appId, serviceId));
    HttpResponse response = restClient.execute(HttpMethod.GET, url);
    return ObjectResponse.fromJsonBody(response, ServiceMeta.class).getResponseObject();
  }
}

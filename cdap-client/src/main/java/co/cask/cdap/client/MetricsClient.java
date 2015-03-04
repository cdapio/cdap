/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.net.URL;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Metrics.
 */
public class MetricsClient {

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public MetricsClient(ClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(config);
  }

  /**
   * Gets the value of a particular metric.
   *
   * @param scope scope of the metric
   * @param context context of the metric
   * @param metric name of the metric
   * @param timeRange time range to query
   * @return value of the metric
   * @throws IOException if a network error occurred
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  // TODO: currently response from metrics endpoint is not "regular", so it's not easy to return an object from here
  // (e.g. metrics endpoint sometimes returns {"data":0} and other times returns {"data":[..]})
  public JsonObject getMetric(String scope, String context, String metric, String timeRange) throws IOException,
    UnauthorizedException {
    URL url = config.resolveURL(String.format("metrics/%s/%s/%s?%s", scope, context, metric, timeRange));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, JsonObject.class).getResponseObject();
  }

}

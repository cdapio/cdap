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
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.http.HttpMethod;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.common.http.ObjectResponse;
import co.cask.cdap.proto.DatasetTypeMeta;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Dataset types.
 */
public class DatasetTypeClient {

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public DatasetTypeClient(ClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(config);
  }

  /**
   * Lists all dataset types.
   *
   * @return list of {@link DatasetTypeMeta}s.
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public List<DatasetTypeMeta> list() throws IOException, UnAuthorizedAccessTokenException {
    URL url = config.resolveURL("data/types");
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<DatasetTypeMeta>>() { }).getResponseObject();
  }

  /**
   * Gets information about a dataset type.
   *
   * @param typeName name of the dataset type
   * @return {@link DatasetTypeMeta} of the dataset type
   * @throws IOException if a network error occurred
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public DatasetTypeMeta get(String typeName) throws IOException, UnAuthorizedAccessTokenException {
    URL url = config.resolveURL(String.format("data/types/%s", typeName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, DatasetTypeMeta.class).getResponseObject();
  }

}

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

import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.client.config.ReactorClientConfig;
import com.continuuity.client.exception.DatasetAlreadyExistsException;
import com.continuuity.client.exception.DatasetNotFoundException;
import com.continuuity.client.exception.DatasetTypeNotFoundException;
import com.continuuity.client.util.RESTClient;
import com.continuuity.common.http.HttpMethod;
import com.continuuity.common.http.HttpRequest;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.common.http.ObjectResponse;
import com.continuuity.proto.DatasetInstanceConfiguration;
import com.continuuity.proto.DatasetMeta;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.inject.Inject;

/**
 * Provides ways to interact with Reactor Datasets.
 */
public class DatasetClient {

  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ReactorClientConfig config;

  @Inject
  public DatasetClient(ReactorClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(config);
  }

  /**
   * Lists all datasets.
   *
   * @return list of {@link DatasetMeta}s.
   * @throws IOException if a network error occurred
   */
  public List<DatasetSpecification> list() throws IOException {
    URL url = config.resolveURL("data/datasets");
    HttpResponse response = restClient.execute(HttpMethod.GET, url);
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<DatasetSpecification>>() { }).getResponseObject();
  }

  /**
   * Creates a dataset.
   *
   * @param datasetName name of the dataset to create
   * @param properties properties of the dataset to create
   * @throws DatasetTypeNotFoundException if the desired dataset type was not found
   * @throws DatasetAlreadyExistsException if a dataset by the same name already exists
   * @throws IOException if a network error occurred
   */
  public void create(String datasetName, DatasetInstanceConfiguration properties)
    throws DatasetTypeNotFoundException, DatasetAlreadyExistsException, IOException {

    URL url = config.resolveURL(String.format("data/datasets/%s", datasetName));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(properties)).build();

    HttpResponse response = restClient.execute(request, HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_CONFLICT);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new DatasetTypeNotFoundException(properties.getTypeName());
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      throw new DatasetAlreadyExistsException(datasetName);
    }
  }

  /**
   * Creates a dataset.
   *
   * @param datasetName Name of the dataset to create
   * @param typeName Type of dataset to create
   * @throws DatasetTypeNotFoundException if the desired dataset type was not found
   * @throws DatasetAlreadyExistsException if a dataset by the same name already exists
   * @throws IOException if a network error occurred
   */
  public void create(String datasetName, String typeName)
    throws DatasetTypeNotFoundException, DatasetAlreadyExistsException, IOException {
    create(datasetName, new DatasetInstanceConfiguration(typeName, ImmutableMap.<String, String>of()));
  }

  /**
   * Deletes a dataset.
   *
   * @param datasetName Name of the dataset to delete
   * @throws DatasetNotFoundException if the dataset with the specified name could not be found
   * @throws IOException if a network error occurred
   */
  public void delete(String datasetName) throws DatasetNotFoundException, IOException {
    URL url = config.resolveURL(String.format("data/datasets/%s", datasetName));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new DatasetNotFoundException(datasetName);
    }
  }

  /**
   * Deletes all datasets. WARNING: This is an unrecoverable operation.
   *
   * @throws IOException if a network error occurred
   */
  public void deleteAll() throws IOException {
    URL url = config.resolveURL("data/unrecoverable/datasets");
    restClient.execute(HttpMethod.DELETE, url);
  }

  /**
   * Truncates a dataset. This will clear all data belonging to the dataset.
   *
   * @param datasetName Name of the dataset to truncate
   * @throws IOException if a network error occurred
   */
  public void truncate(String datasetName) throws IOException {
    URL url = config.resolveURL(String.format("data/datasets/%s/admin/truncate", datasetName));
    restClient.execute(HttpMethod.POST, url);
  }

}

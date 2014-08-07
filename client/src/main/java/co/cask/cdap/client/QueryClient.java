/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.client.exception.BadRequestException;
import co.cask.cdap.client.exception.QueryNotFoundException;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.http.HttpMethod;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.common.http.ObjectResponse;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.inject.Inject;

/**
 * Provides ways to query Reactor Datasets.
 */
public class QueryClient {

  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public QueryClient(ClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(config);
  }

  /**
   * Executes a query asynchronously.
   *
   * @param query query string to execute
   * @return {@link QueryHandle} to use when fetching the status and result of the query.
   * See {@link #getStatus(QueryHandle)}, {@link #getSchema(QueryHandle)}, and {@link #getResults(QueryHandle, int)}.
   * @throws IOException if a network error occurred
   * @throws BadRequestException if the query was malformed
   */
  public QueryHandle execute(String query) throws IOException, BadRequestException {
    URL url = config.resolveURL("data/explore/queries");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(ImmutableMap.of("query", query))).build();

    HttpResponse response = restClient.execute(request, HttpURLConnection.HTTP_BAD_REQUEST);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("The query is not well-formed or contains an error, " +
                                      "such as a nonexistent table name: " + query);
    }

    return ObjectResponse.fromJsonBody(response, QueryHandle.class).getResponseObject();
  }

  /**
   * Gets the status of a query.
   *
   * @param queryHandle {@link QueryHandle} from {@link #execute(String)}
   * @return status of the query
   * @throws IOException if a network error occurred
   * @throws QueryNotFoundException if the query with the specified handle was not found
   */
  public QueryStatus getStatus(QueryHandle queryHandle) throws IOException, QueryNotFoundException {
    URL url = config.resolveURL(String.format("data/explore/queries/%s/status", queryHandle.getHandle()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new QueryNotFoundException(queryHandle.getHandle());
    }

    return ObjectResponse.fromJsonBody(response, QueryStatus.class).getResponseObject();
  }

  /**
   * Gets the schema of a query result.
   *
   * @param queryHandle {@link QueryHandle} from {@link #execute(String)}
   * @return schema of the query result
   * @throws IOException if a network error occurred
   * @throws QueryNotFoundException if the query with the specified handle was not found
   */
  public List<ColumnDesc> getSchema(QueryHandle queryHandle)
    throws IOException, QueryNotFoundException {

    URL url = config.resolveURL(String.format("data/explore/queries/%s/schema", queryHandle.getHandle()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new QueryNotFoundException(queryHandle.getHandle());
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ColumnDesc>>() { })
      .getResponseObject();
  }

  /**
   * Gets the results of a query.
   *
   * @param queryHandle {@link QueryHandle} from {@link #execute(String)}
   * @param batchSize number of rows to fetch per batch
   * @return list of rows
   * @throws IOException if a network error occurred
   * @throws QueryNotFoundException if the query with the specified handle was not found
   */
  public List<QueryResult> getResults(QueryHandle queryHandle, int batchSize)
    throws IOException, QueryNotFoundException {

    URL url = config.resolveURL(String.format("data/explore/queries/%s/next", queryHandle.getHandle()));
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(ImmutableMap.of("size", batchSize))).build();

    HttpResponse response = restClient.execute(request, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new QueryNotFoundException(queryHandle.getHandle());
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<List<QueryResult>>() { }).getResponseObject();
  }

  /**
   * Deletes a query.
   *
   * @param queryHandle {@link QueryHandle} from {@link #execute(String)}
   * @throws IOException if a network error occurred
   * @throws QueryNotFoundException if the query with the specified handle was not found
   * @throws BadRequestException if the query could not be deleted at the moment
   */
  public void delete(QueryHandle queryHandle) throws IOException, QueryNotFoundException, BadRequestException {
    URL url = config.resolveURL(String.format("data/explore/queries/%s", queryHandle.getHandle()));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url,
                                               HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_BAD_REQUEST);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new QueryNotFoundException(queryHandle.getHandle());
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("The query '" + queryHandle + "' was not in a state that could be closed;" +
                                      " either wait until it is finished, or cancel it");
    }
  }

  /**
   * Cancels a query.
   *
   * @param queryHandle {@link QueryHandle} from {@link #execute(String)}
   * @throws IOException if a network error occurred
   * @throws QueryNotFoundException if the query with the specified handle was not found
   * @throws BadRequestException if the query was not in a state that could be canceled
   */
  public void cancel(QueryHandle queryHandle) throws IOException, QueryNotFoundException, BadRequestException {
    URL url = config.resolveURL(String.format("data/explore/queries/%s/cancel", queryHandle.getHandle()));
    HttpResponse response = restClient.execute(HttpMethod.POST, url,
                                               HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_BAD_REQUEST);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new QueryNotFoundException(queryHandle.getHandle());
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("The query '" + queryHandle + "' was not in a state that can be canceled");
    }
  }
}

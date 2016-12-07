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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ViewDetail;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP stream views.
 */
@Beta
public class StreamViewClient {

  private static final TypeToken<List<String>> LIST_TYPE = new TypeToken<List<String>>() { };
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public StreamViewClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public StreamViewClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  /**
   * Creates or updates a view.
   *
   * @param id the view
   * @param viewSpecification the view config
   * @return true if a view was created, false if updated
   * @deprecated since 4.0.0. Use {@link #createOrUpdate(StreamViewId, ViewSpecification)} instead.
   */
  @Deprecated
  public boolean createOrUpdate(Id.Stream.View id, ViewSpecification viewSpecification)
    throws NotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
    return createOrUpdate(id.toEntityId(), viewSpecification);
  }

  /**
   * Creates or updates a view.
   *
   * @param id the view
   * @param viewSpecification the view config
   * @return true if a view was created, false if updated
   */
  public boolean createOrUpdate(StreamViewId id, ViewSpecification viewSpecification)
    throws NotFoundException, IOException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(
      id.getParent().getParent(), String.format("streams/%s/views/%s", id.getStream(), id.getView()));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(viewSpecification)).build();
    HttpResponse response = restClient.execute(request, config.getAccessToken());
    return response.getResponseCode() == 201;
  }

  /**
   * Deletes a view.
   *
   * @param id the view
   * @throws NotFoundException if the view was not found
   * @deprecated since 4.0.0. Use {@link #delete(StreamViewId)} instead.
   */
  @Deprecated
  public void delete(Id.Stream.View id)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {
    delete(id.toEntityId());
  }

  /**
   * Deletes a view.
   *
   * @param id the view
   * @throws NotFoundException if the view was not found
   */
  public void delete(StreamViewId id)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(
      id.getParent().getParent(), String.format("streams/%s/views/%s", id.getStream(), id.getView()));
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(id);
    }
  }

  /**
   * Lists all views associated with a stream.
   *
   * @return the list of views associated with a stream
   * @deprecated since 4.0.0. Use {@link #list(StreamId)} instead.
   */
  @Deprecated
  public List<String> list(Id.Stream stream) throws IOException, UnauthenticatedException, UnauthorizedException {
    return list(stream.toEntityId());
  }

  /**
   * Lists all views associated with a stream.
   *
   * @return the list of views associated with a stream
   */
  public List<String> list(StreamId stream) throws IOException, UnauthenticatedException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(stream.getParent(), String.format("streams/%s/views", stream.getStream()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    return ObjectResponse.fromJsonBody(response, LIST_TYPE).getResponseObject();
  }

  /**
   * Gets detailed information about a view.
   *
   * @param id the view
   * @return the detailed information about the view
   * @throws NotFoundException if the view was not found
   * @deprecated since 4.0.0. Use {@link #get(StreamViewId)} instead.
   */
  @Deprecated
  public ViewDetail get(Id.Stream.View id)
    throws NotFoundException, IOException, UnauthenticatedException, UnauthorizedException {
    return get(id.toEntityId());
  }

  /**
   * Gets detailed information about a view.
   *
   * @param id the view
   * @return the detailed information about the view
   * @throws NotFoundException if the view was not found
   */
  public ViewDetail get(StreamViewId id)
    throws NotFoundException, IOException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(
      id.getParent().getParent(), String.format("streams/%s/views/%s", id.getStream(), id.getView()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(id);
    }
    return ObjectResponse.fromJsonBody(response, ViewDetail.class, GSON).getResponseObject();
  }
}

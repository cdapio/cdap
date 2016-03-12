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
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Lineage.
 */
public class LineageClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec()).create();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public LineageClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public LineageClient(ClientConfig config) {
    this(config, new RESTClient());
  }

  /**
   * Retrieves Lineage for a given dataset.
   *
   * @param datasetInstance the dataset for which to retrieve lineage
   * @param startTime start time for the query, in seconds
   * @param endTime end time for the query, in seconds
   * @param levels number of levels to compute lineage for, or {@code null} to use the LineageHandler's default value
   * @return {@link LineageRecord} for the specified dataset.
   */
  public LineageRecord getLineage(Id.DatasetInstance datasetInstance, long startTime, long endTime,
                                  @Nullable Integer levels)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    return getLineage(datasetInstance, Long.toString(startTime), Long.toString(endTime), levels);
  }

  /**
   * Retrieves Lineage for a given dataset.
   *
   * @param datasetInstance the dataset for which to retrieve lineage
   * @param startTime start time for the query, in seconds, or in 'now - xs' format
   * @param endTime end time for the query, in seconds, or in 'now - xs' format
   * @param levels number of levels to compute lineage for, or {@code null} to use the LineageHandler's default value
   * @return {@link LineageRecord} for the specified dataset.
   */
  public LineageRecord getLineage(Id.DatasetInstance datasetInstance, String startTime, String endTime,
                                  @Nullable Integer levels)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    String path = String.format("datasets/%s/lineage?start=%s&end=%s", datasetInstance.getId(),
                                URLEncoder.encode(startTime, "UTF-8"), URLEncoder.encode(endTime, "UTF-8"));
    if (levels != null) {
      path = String.format("%s&levels=%d", path, levels);
    }
    return getLineage(datasetInstance, path);
  }

  /**
   * Retrieves Lineage for a given stream.
   *
   * @param streamId the stream for which to retrieve lineage
   * @param startTime start time for the query, in seconds
   * @param endTime end time for the query, in seconds
   * @param levels number of levels to compute lineage for, or {@code null} to use the LineageHandler's default value
   * @return {@link LineageRecord} for the specified stream.
   */
  public LineageRecord getLineage(Id.Stream streamId, long startTime, long endTime, @Nullable Integer levels)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    return getLineage(streamId, Long.toString(startTime), Long.toString(endTime), levels);
  }

  /**
   * Retrieves Lineage for a given stream.
   *
   * @param streamId the stream for which to retrieve lineage
   * @param startTime start time for the query, in seconds, or in 'now - xs' format
   * @param endTime end time for the query, in seconds, or in 'now - xs' format
   * @param levels number of levels to compute lineage for, or {@code null} to use the LineageHandler's default value
   * @return {@link LineageRecord} for the specified stream.
   */
  public LineageRecord getLineage(Id.Stream streamId, String startTime, String endTime, @Nullable Integer levels)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    String path = String.format("streams/%s/lineage?start=%s&end=%s", streamId.getId(),
                                URLEncoder.encode(startTime, "UTF-8"), URLEncoder.encode(endTime, "UTF-8"));
    if (levels != null) {
      path = String.format("%s&levels=%d", path, levels);
    }
    return getLineage(streamId, path);
  }

  private LineageRecord getLineage(Id.NamespacedId namespacedId, String path)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    URL lineageURL = config.resolveNamespacedURLV3(namespacedId.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpRequest.get(lineageURL).build(),
                                               config.getAccessToken(),
                                               HttpURLConnection.HTTP_BAD_REQUEST, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(namespacedId);
    }
    return GSON.fromJson(response.getResponseBodyAsString(), LineageRecord.class);
  }
}

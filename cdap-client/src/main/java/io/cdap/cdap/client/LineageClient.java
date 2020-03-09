/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.codec.NamespacedEntityIdCodec;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.metadata.lineage.CollapseType;
import io.cdap.cdap.proto.metadata.lineage.FieldLineageSummary;
import io.cdap.cdap.proto.metadata.lineage.LineageRecord;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Lineage.
 */
public class LineageClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();

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
  public LineageRecord getLineage(DatasetId datasetInstance, long startTime, long endTime,
                                  @Nullable Integer levels)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
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
  public LineageRecord getLineage(DatasetId datasetInstance, String startTime, String endTime,
                                  @Nullable Integer levels)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getLineage(datasetInstance, startTime, endTime, Collections.<CollapseType>emptySet(), levels);
  }

  /**
   * Retrieves Lineage for a given dataset.
   *
   * @param datasetInstance the dataset for which to retrieve lineage
   * @param startTime start time for the query, in seconds
   * @param endTime end time for the query, in seconds
   * @param collapseTypes fields on which lineage relations can be collapsed on
   * @param levels number of levels to compute lineage for, or {@code null} to use the LineageHandler's default value
   * @return {@link LineageRecord} for the specified dataset.
   */
  public LineageRecord getLineage(DatasetId datasetInstance, long startTime, long endTime,
                                  Set<CollapseType> collapseTypes, @Nullable Integer levels)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getLineage(datasetInstance, Long.toString(startTime), Long.toString(endTime), collapseTypes, levels);
  }

  /**
   * Retrieves Lineage for a given dataset.
   *
   * @param datasetInstance the dataset for which to retrieve lineage
   * @param startTime start time for the query, in seconds, or in 'now - xs' format
   * @param endTime end time for the query, in seconds, or in 'now - xs' format
   * @param collapseTypes fields on which lineage relations can be collapsed on
   * @param levels number of levels to compute lineage for, or {@code null} to use the LineageHandler's default value
   * @return {@link LineageRecord} for the specified dataset.
   */
  public LineageRecord getLineage(DatasetId datasetInstance, String startTime, String endTime,
                                  Set<CollapseType> collapseTypes, @Nullable Integer levels)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("datasets/%s/lineage?start=%s&end=%s", datasetInstance.getDataset(),
                                URLEncoder.encode(startTime, "UTF-8"), URLEncoder.encode(endTime, "UTF-8"));
    for (CollapseType collapseType : collapseTypes) {
      path = String.format("%s&collapse=%s", path, collapseType);
    }
    if (levels != null) {
      path = String.format("%s&levels=%d", path, levels);
    }
    return getLineage(datasetInstance, path, LineageRecord.class);
  }

  /**
   * Retrieves field lineage about a dataset
   *
   * @param datasetId the dataset for which to retrieve lineage
   * @param startTime start time for the query, in milliseconds, or in 'now - xs' format
   * @param endTime end time for the query, in milliseconds, or in 'now - xs' format
   * @param direction direction of the field lineage, can be incoming, outgoing or both
   * @return {@link FieldLineageSummary} for the specified dataset.
   */
  public FieldLineageSummary getFieldLineage(DatasetId datasetId, long startTime, long endTime,
                                             String direction)
    throws IOException, BadRequestException, NotFoundException, UnauthenticatedException {
    String path = String.format("datasets/%s/lineage/allfieldlineage?start=%s&end=%s", datasetId.getDataset(),
                                URLEncoder.encode(Long.toString(startTime), "UTF-8"),
                                URLEncoder.encode(Long.toString(endTime), "UTF-8"));
    if (direction != null) {
      path = String.format("%s&direction=%s", path, URLEncoder.encode(direction, "UTF-8"));
    }
    return getLineage(datasetId, path, FieldLineageSummary.class);
  }

  private <T> T getLineage(NamespacedEntityId namespacedId, String path, Class<T> type)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    URL lineageURL = config.resolveNamespacedURLV3(new NamespaceId(namespacedId.getNamespace()), path);
    HttpResponse response = restClient.execute(HttpRequest.get(lineageURL).build(),
                                               config.getAccessToken(),
                                               HttpURLConnection.HTTP_BAD_REQUEST, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(response.getResponseBodyAsString());
    }
    return GSON.fromJson(response.getResponseBodyAsString(), type);
  }
}

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
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Metadata.
 */
public class MetadataClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec()).create();
  private static final Type SET_METADATA_RECORD_TYPE = new TypeToken<Set<MetadataRecord>>() { }.getType();
  private static final Type SET_METADATA_SEARCH_RESULT_TYPE =
    new TypeToken<Set<MetadataSearchResultRecord>>() { }.getType();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public MetadataClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public MetadataClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  /**
   * Returns search results for metadata.
   *
   * @param namespace the namespace to search in
   * @param query the query string with which to search
   * @param target the target type. If null, all possible types will be searched
   * @return A set of {@link MetadataSearchResultRecord} for the given query.
   */
  public Set<MetadataSearchResultRecord> searchMetadata(Id.Namespace namespace, String query,
                                                        @Nullable MetadataSearchTargetType target)
    throws IOException, UnauthorizedException {

    String path = String.format("metadata/search?query=%s", query);
    if (target != null) {
      path = path + "&target=" + target.getInternalName();
    }
    URL searchURL = config.resolveNamespacedURLV3(namespace, path);
    HttpResponse response = restClient.execute(HttpRequest.get(searchURL).build(),
                                               config.getAccessToken());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_SEARCH_RESULT_TYPE);
  }


  /**
   * @param appId the app for which to retrieve metadata
   * @return The metadata for the application.
   */
  public Set<MetadataRecord> getMetadata(Id.Application appId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/metadata", appId.getId());
    return getMetadata(appId, path);
  }

  /**
   * @param datasetInstance the dataset for which to retrieve metadata
   * @return The metadata for the dataset.
   */
  public Set<MetadataRecord> getMetadata(Id.DatasetInstance datasetInstance)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("datasets/%s/metadata", datasetInstance.getId());
    return getMetadata(datasetInstance, path);
  }

  /**
   * @param streamId the stream for which to retrieve metadata
   * @return The metadata for the stream.
   */
  public Set<MetadataRecord> getMetadata(Id.Stream streamId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("streams/%s/metadata", streamId.getId());
    return getMetadata(streamId, path);
  }

  /**
   * @param programId the program for which to retrieve metadata
   * @return The metadata for the program.
   */
  public Set<MetadataRecord> getMetadata(Id.Program programId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/%s/%s/metadata",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
    return getMetadata(programId, path);
  }

  /**
   * @param runId the run for which to retrieve metadata
   * @return The metadata for the run.
   */
  public Set<MetadataRecord> getMetadata(Id.Run runId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    Id.Program programId = runId.getProgram();
    String path = String.format("apps/%s/%s/%s/runs/%s/metadata",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId(),
                                runId.getId());
    return getMetadata(runId, path);
  }

  private Set<MetadataRecord> getMetadata(Id.NamespacedId namespacedId, String path)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    HttpResponse response = makeRequest(namespacedId, path, HttpMethod.GET);
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_RECORD_TYPE);
  }

  /**
   * Adds tags to an application.
   *
   * @param appId app to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Application appId, Set<String> tags)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/metadata/tags", appId.getId());
    addTags(appId, path, tags);
  }

  /**
   * Adds tags to a dataset.
   *
   * @param datasetInstance dataset to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.DatasetInstance datasetInstance, Set<String> tags)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("datasets/%s/metadata/tags", datasetInstance.getId());
    addTags(datasetInstance, path, tags);
  }

  /**
   * Adds tags to a stream.
   *
   * @param streamId stream to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Stream streamId, Set<String> tags)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("streams/%s/metadata/tags", streamId.getId());
    addTags(streamId, path, tags);
  }

  /**
   * Adds tags to a program.
   *
   * @param programId program to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Program programId, Set<String> tags)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/%s/%s/metadata/tags",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
    addTags(programId, path, tags);
  }

  private void addTags(Id.NamespacedId namespacedId, String path, Set<String> tags)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    makeRequest(namespacedId, path, HttpMethod.POST, GSON.toJson(tags));
  }

  /**
   * Adds properties to an application.
   *
   * @param appId app to add tags to
   * @param properties tags to be added
   */
  public void addProperties(Id.Application appId, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/metadata/properties", appId.getId());
    addProperties(appId, path, properties);
  }

  /**
   * Adds properties to a dataset.
   *
   * @param datasetInstance dataset to add tags to
   * @param properties tags to be added
   */
  public void addProperties(Id.DatasetInstance datasetInstance, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("datasets/%s/metadata/properties", datasetInstance.getId());
    addProperties(datasetInstance, path, properties);
  }

  /**
   * Adds properties to a stream.
   *
   * @param streamId stream to add tags to
   * @param properties tags to be added
   */
  public void addProperties(Id.Stream streamId, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("streams/%s/metadata/properties", streamId.getId());
    addProperties(streamId, path, properties);
  }

  /**
   * Adds properties to a program.
   *
   * @param programId program to add tags to
   * @param properties tags to be added
   */
  public void addProperties(Id.Program programId, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/%s/%s/metadata/properties",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
    addProperties(programId, path, properties);
  }

  private void addProperties(Id.NamespacedId namespacedId, String path, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    makeRequest(namespacedId, path, HttpMethod.POST, GSON.toJson(properties));
  }

  /**
   * Removes properties from an application.
   *
   * @param appId app to remove properties from
   * @param propertyToRemove property to be removed, or {@code null} to remove all properties for the entity
   */
  public void removeProperties(Id.Application appId, @Nullable String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/metadata/properties", appId.getId());
    removeProperties(appId, path, propertyToRemove);
  }

  /**
   * Removes properties from a dataset.
   *
   * @param datasetInstance dataset to remove properties from
   * @param propertyToRemove property to be removed, or {@code null} to remove all properties for the entity
   */
  public void removeProperties(Id.DatasetInstance datasetInstance, @Nullable String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("datasets/%s/metadata/properties", datasetInstance.getId());
    removeProperties(datasetInstance, path, propertyToRemove);
  }

  /**
   * Removes properties from a stream.
   *
   * @param streamId stream to remove properties from
   * @param propertyToRemove property to be removed, or {@code null} to remove all properties for the entity
   */
  public void removeProperties(Id.Stream streamId, @Nullable String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("streams/%s/metadata/properties", streamId.getId());
    removeProperties(streamId, path, propertyToRemove);
  }

  /**
   * Removes properties from a program.
   *
   * @param programId program to remove properties from
   * @param propertyToRemove property to be removed, or {@code null} to remove all properties for the entity
   */
  public void removeProperties(Id.Program programId, @Nullable String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/%s/%s/metadata/properties",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
    removeProperties(programId, path, propertyToRemove);
  }

  private void removeProperties(Id.NamespacedId namespacedId, String path, @Nullable String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    if (propertyToRemove != null) {
      path = path + "/" + propertyToRemove;
    }
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

  /**
   * Removes tags from an application.
   *
   * @param appId app to remove tags from
   * @param tagToRemove tag to be removed, or {@code null} to remove all tags for the entity
   */
  public void removeTags(Id.Application appId, @Nullable String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/metadata/tags", appId.getId());
    removeTags(appId, path, tagToRemove);
  }

  /**
   * Removes tags from a dataset.
   *
   * @param datasetInstance dataset to remove tags from
   * @param tagToRemove tag to be removed, or {@code null} to remove all tags for the entity
   */
  public void removeTags(Id.DatasetInstance datasetInstance, @Nullable String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("datasets/%s/metadata/tags", datasetInstance.getId());
    removeTags(datasetInstance, path, tagToRemove);
  }

  /**
   * Removes tags from a stream.
   *
   * @param streamId stream to remove tags from
   * @param tagToRemove tag to be removed, or {@code null} to remove all tags for the entity
   */
  public void removeTags(Id.Stream streamId, @Nullable String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("streams/%s/metadata/tags", streamId.getId());
    removeTags(streamId, path, tagToRemove);
  }

  /**
   * Removes tags from a program.
   *
   * @param programId program to remove tags from
   * @param tagToRemove tag to be removed, or {@code null} to remove all tags for the entity
   */
  public void removeTags(Id.Program programId, @Nullable String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/%s/%s/metadata/tags",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
    removeTags(programId, path, tagToRemove);
  }

  private void removeTags(Id.NamespacedId namespacedId, String path, @Nullable String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    if (tagToRemove != null) {
      path = path + "/" + tagToRemove;
    }
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

 /**
  * Removes metadata from an application.
  *
  * @param appId app to remove metadata from
  */
  public void removeMetadata(Id.Application appId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/metadata", appId.getId());
    makeRequest(appId, path, HttpMethod.DELETE);
  }

  /**
   * Removes metadata from a dataset.
   *
   * @param datasetInstance dataset to remove metadata from
   */
  public void removeMetadata(Id.DatasetInstance datasetInstance)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("datasets/%s/metadata", datasetInstance.getId());
    makeRequest(datasetInstance, path, HttpMethod.DELETE);
  }

  /**
   * Removes metadata from a stream.
   *
   * @param streamId stream to remove metadata from
   */
  public void removeMetadata(Id.Stream streamId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("streams/%s/metadata", streamId.getId());
    makeRequest(streamId, path, HttpMethod.DELETE);
  }

  /**
   * Removes metadata from a program.
   *
   * @param programId program to remove metadata from
   */
  public void removeMetadata(Id.Program programId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("apps/%s/%s/%s/metadata",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
    makeRequest(programId, path, HttpMethod.DELETE);
  }

  private HttpResponse makeRequest(Id.NamespacedId namespacedId, String path, HttpMethod httpMethod)
    throws NotFoundException, BadRequestException, UnauthorizedException, IOException {
    return makeRequest(namespacedId, path, httpMethod, null);
  }

  // makes a request and throws BadRequestException or NotFoundException, as appropriate
  private HttpResponse makeRequest(Id.NamespacedId namespacedId, String path,
                                   HttpMethod httpMethod, @Nullable String body)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    URL url = config.resolveNamespacedURLV3(namespacedId.getNamespace(), path);
    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url);
    if (body != null) {
      builder.withBody(body);
    }
    HttpResponse response = restClient.execute(builder.build(), config.getAccessToken(),
                                               HttpURLConnection.HTTP_BAD_REQUEST, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(namespacedId);
    }
    return response;
  }
}

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
    return getMetadata(appId, constructPath(appId));
  }

  /**
   * @param artifactId the artifact for which to retrieve metadata
   * @return The metadata for the artifact.
   */
  public Set<MetadataRecord> getMetadata(Id.Artifact artifactId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    return getMetadata(artifactId, constructPath(artifactId));
  }

  /**
   * @param datasetInstance the dataset for which to retrieve metadata
   * @return The metadata for the dataset.
   */
  public Set<MetadataRecord> getMetadata(Id.DatasetInstance datasetInstance)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    return getMetadata(datasetInstance, constructPath(datasetInstance));
  }

  /**
   * @param streamId the stream for which to retrieve metadata
   * @return The metadata for the stream.
   */
  public Set<MetadataRecord> getMetadata(Id.Stream streamId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    return getMetadata(streamId, constructPath(streamId));
  }

  /**
   * @param programId the program for which to retrieve metadata
   * @return The metadata for the program.
   */
  public Set<MetadataRecord> getMetadata(Id.Program programId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    return getMetadata(programId, constructPath(programId));
  }

  /**
   * @param runId the run for which to retrieve metadata
   * @return The metadata for the run.
   */
  public Set<MetadataRecord> getMetadata(Id.Run runId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    return getMetadata(runId, constructPath(runId));
  }

  private Set<MetadataRecord> getMetadata(Id.NamespacedId namespacedId, String entityPath)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("%s/metadata", entityPath);
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
    addTags(appId, constructPath(appId), tags);
  }

  /**
   * Adds tags to an artifact.
   *
   * @param artifactId artifact to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Artifact artifactId, Set<String> tags)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    addTags(artifactId, constructPath(artifactId), tags);
  }

  /**
   * Adds tags to a dataset.
   *
   * @param datasetInstance dataset to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.DatasetInstance datasetInstance, Set<String> tags)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    addTags(datasetInstance, constructPath(datasetInstance), tags);
  }

  /**
   * Adds tags to a stream.
   *
   * @param streamId stream to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Stream streamId, Set<String> tags)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    addTags(streamId, constructPath(streamId), tags);
  }

  /**
   * Adds tags to a program.
   *
   * @param programId program to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Program programId, Set<String> tags)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    addTags(programId, constructPath(programId), tags);
  }

  private void addTags(Id.NamespacedId namespacedId, String entityPath, Set<String> tags)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("%s/metadata/tags", entityPath);
    makeRequest(namespacedId, path, HttpMethod.POST, GSON.toJson(tags));
  }

  /**
   * Adds properties to an application.
   *
   * @param appId app to add properties to
   * @param properties properties to be added
   */
  public void addProperties(Id.Application appId, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    addProperties(appId, constructPath(appId), properties);
  }

  /**
   * Adds properties to an artifact.
   *
   * @param artifactId artifact to add properties to
   * @param properties properties to be added
   */
  public void addProperties(Id.Artifact artifactId, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    addProperties(artifactId, constructPath(artifactId), properties);
  }

  /**
   * Adds properties to a dataset.
   *
   * @param datasetInstance dataset to add properties to
   * @param properties properties to be added
   */
  public void addProperties(Id.DatasetInstance datasetInstance, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    addProperties(datasetInstance, constructPath(datasetInstance), properties);
  }

  /**
   * Adds properties to a stream.
   *
   * @param streamId stream to add properties to
   * @param properties properties to be added
   */
  public void addProperties(Id.Stream streamId, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    addProperties(streamId, constructPath(streamId), properties);
  }

  /**
   * Adds properties to a program.
   *
   * @param programId program to add properties to
   * @param properties properties to be added
   */
  public void addProperties(Id.Program programId, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    addProperties(programId, constructPath(programId), properties);
  }

  private void addProperties(Id.NamespacedId namespacedId, String entityPath, Map<String, String> properties)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("%s/metadata/properties", entityPath);
    makeRequest(namespacedId, path, HttpMethod.POST, GSON.toJson(properties));
  }

  /**
   * Removes a property from an application.
   *
   * @param appId app to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(Id.Application appId, String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeProperty(appId, constructPath(appId), propertyToRemove);
  }

  /**
   * Removes a property from an artifact.
   *
   * @param artifactId artifact to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(Id.Artifact artifactId, String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeProperty(artifactId, constructPath(artifactId), propertyToRemove);
  }

  /**
   * Removes a property from a dataset.
   *
   * @param datasetInstance dataset to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(Id.DatasetInstance datasetInstance, String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeProperty(datasetInstance, constructPath(datasetInstance), propertyToRemove);
  }

  /**
   * Removes a property from a stream.
   *
   * @param streamId stream to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(Id.Stream streamId, String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeProperty(streamId, constructPath(streamId), propertyToRemove);
  }

  /**
   * Removes a property from a program.
   *
   * @param programId program to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(Id.Program programId, String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeProperty(programId, constructPath(programId), propertyToRemove);
  }

  private void removeProperty(Id.NamespacedId namespacedId, String entityPath, String propertyToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("%s/metadata/properties/%s", entityPath, propertyToRemove);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }


  /**
   * Removes all properties from an application.
   *
   * @param appId app to remove properties from
   */
  public void removeProperties(Id.Application appId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeProperties(appId, constructPath(appId));
  }

  /**
   * Removes all properties from an artifact.
   *
   * @param artifactId artifact to remove properties from
   */
  public void removeProperties(Id.Artifact artifactId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeProperties(artifactId, constructPath(artifactId));
  }

  /**
   * Removes all properties from a dataset.
   *
   * @param datasetInstance dataset to remove properties from
   */
  public void removeProperties(Id.DatasetInstance datasetInstance)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeProperties(datasetInstance, constructPath(datasetInstance));
  }

  /**
   * Removes all properties from a stream.
   *
   * @param streamId stream to remove properties from
   */
  public void removeProperties(Id.Stream streamId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeProperties(streamId, constructPath(streamId));
  }

  /**
   * Removes all properties from a program.
   *
   * @param programId program to remove properties from
   */
  public void removeProperties(Id.Program programId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeProperties(programId, constructPath(programId));
  }

  private void removeProperties(Id.NamespacedId namespacedId, String entityPath)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("%s/metadata/properties", entityPath);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

  /**
   * Removes a tag from an application.
   *
   * @param appId app to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(Id.Application appId, String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeTag(appId, constructPath(appId), tagToRemove);
  }

  /**
   * Removes a tag from an artifact.
   *
   * @param artifactId artifact to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(Id.Artifact artifactId, String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeTag(artifactId, constructPath(artifactId), tagToRemove);
  }


  /**
   * Removes a tag from a dataset.
   *
   * @param datasetInstance dataset to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(Id.DatasetInstance datasetInstance, String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeTag(datasetInstance, constructPath(datasetInstance), tagToRemove);
  }

  /**
   * Removes a tag from a stream.
   *
   * @param streamId stream to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(Id.Stream streamId, String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeTag(streamId, constructPath(streamId), tagToRemove);
  }

  /**
   * Removes a tag from a program.
   *
   * @param programId program to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(Id.Program programId, String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeTag(programId, constructPath(programId), tagToRemove);
  }

  private void removeTag(Id.NamespacedId namespacedId, String entityPath, String tagToRemove)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("%s/metadata/tags/%s", entityPath, tagToRemove);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

  /**
   * Removes all tags from an application.
   *
   * @param appId app to remove tags from
   */
  public void removeTags(Id.Application appId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeTags(appId, constructPath(appId));
  }

  /**
   * Removes all tags from an artifact.
   *
   * @param artifactId artifact to remove tags from
   */
  public void removeTags(Id.Artifact artifactId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeTags(artifactId, constructPath(artifactId));
  }

  /**
   * Removes all tags from a dataset.
   *
   * @param datasetInstance dataset to remove tags from
   */
  public void removeTags(Id.DatasetInstance datasetInstance)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeTags(datasetInstance, constructPath(datasetInstance));
  }

  /**
   * Removes all tags from a stream.
   *
   * @param streamId stream to remove tags from
   */
  public void removeTags(Id.Stream streamId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeTags(streamId, constructPath(streamId));
  }

  /**
   * Removes all tags from a program.
   *
   * @param programId program to remove tags from
   */
  public void removeTags(Id.Program programId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeTags(programId, constructPath(programId));
  }

  private void removeTags(Id.NamespacedId namespacedId, String entityPath)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    String path = String.format("%s/metadata/tags", entityPath);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

 /**
  * Removes metadata from an application.
  *
  * @param appId app to remove metadata from
  */
  public void removeMetadata(Id.Application appId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeMetadata(appId, constructPath(appId));
  }

  /**
   * Removes metadata from an artifact.
   *
   * @param artifactId artifact to remove metadata from
   */
  public void removeMetadata(Id.Artifact artifactId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeMetadata(artifactId, constructPath(artifactId));
  }

  /**
   * Removes metadata from a dataset.
   *
   * @param datasetInstance dataset to remove metadata from
   */
  public void removeMetadata(Id.DatasetInstance datasetInstance)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeMetadata(datasetInstance, constructPath(datasetInstance));
  }

  /**
   * Removes metadata from a stream.
   *
   * @param streamId stream to remove metadata from
   */
  public void removeMetadata(Id.Stream streamId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeMetadata(streamId, constructPath(streamId));
  }

  /**
   * Removes metadata from a program.
   *
   * @param programId program to remove metadata from
   */
  public void removeMetadata(Id.Program programId)
    throws IOException, UnauthorizedException, NotFoundException, BadRequestException {
    removeMetadata(programId, constructPath(programId));
  }

  private void removeMetadata(Id.NamespacedId namespacedId, String entityPath)
    throws UnauthorizedException, BadRequestException, NotFoundException, IOException {
    String path = String.format("%s/metadata", entityPath);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
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

  // construct a component of the path, specific to each entity type

  private String constructPath(Id.Application appId) {
    return String.format("apps/%s", appId.getId());
  }

  private String constructPath(Id.Artifact artifactId) {
    return String.format("artifacts/%s/versions/%s", artifactId.getName(), artifactId.getVersion().getVersion());
  }

  private String constructPath(Id.Program programId) {
    return String.format("apps/%s/%s/%s",
                         programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
  }

  private String constructPath(Id.Run runId) {
    Id.Program programId = runId.getProgram();
    return String.format("apps/%s/%s/%s/runs/%s",
                         programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId(),
                         runId.getId());
  }

  private String constructPath(Id.DatasetInstance datasetInstance) {
    return String.format("datasets/%s", datasetInstance.getId());
  }

  private String constructPath(Id.Stream streamId) {
    return String.format("streams/%s", streamId.getId());
  }
}

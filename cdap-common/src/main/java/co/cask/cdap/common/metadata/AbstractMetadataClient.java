/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.metadata;

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import scala.App;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Common implementation of methods to interact with metadata service over HTTP.
 */
public abstract class AbstractMetadataClient {
  private static final Type SET_METADATA_RECORD_TYPE = new TypeToken<Set<MetadataRecord>>() { }.getType();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type SET_STRING_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();

  /**
   * Executes an HTTP request.
   */
  protected abstract HttpResponse execute(HttpRequest request,  int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, UnauthorizedException;

  /**
   * Resolves the specified URL.
   */
  protected abstract URL resolve(NamespaceId namesapace, String resource) throws IOException;

  /**
   * Searches entities in the specified namespace whose metadata matches the specified query.
   *
   * @param namespace the namespace to search in
   * @param query the query string with which to search
   * @param target the target type. If null, all possible types will be searched
   * @return the {@link MetadataSearchResponse} for the given query.
   */
  public MetadataSearchResponse searchMetadata(NamespaceId namespace, String query,
                                               @Nullable MetadataSearchTargetType target)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    Set<MetadataSearchTargetType> targets = ImmutableSet.of();
    if (target != null) {
      targets = ImmutableSet.of(target);
    }
    return searchMetadata(namespace, query, targets);
  }


  /**
   * Searches entities in the specified namespace whose metadata matches the specified query.
   *
   * @param namespace the namespace to search in
   * @param query the query string with which to search
   * @param targets {@link MetadataSearchTargetType}s to search. If empty, all possible types will be searched
   * @return A set of {@link MetadataSearchResultRecord} for the given query.
   */
  public MetadataSearchResponse searchMetadata(NamespaceId namespace, String query,
                                               Set<MetadataSearchTargetType> targets)
    throws IOException, UnauthenticatedException, UnauthorizedException {

    String path = String.format("metadata/search?query=%s", query);
    for (MetadataSearchTargetType t : targets) {
      path += "&target=" + t;
    }
    URL searchURL = resolve(namespace, path);
    HttpResponse response = execute(HttpRequest.get(searchURL).build());
    return GSON.fromJson(response.getResponseBodyAsString(), MetadataSearchResponse.class);
  }

  /**
   * @param id the entity for which to retrieve metadata across {@link MetadataScope#SYSTEM} and
   * {@link MetadataScope#USER}
   * @return The metadata for the entity.
   */
  public Set<MetadataRecord> getMetadata(EntityId id)
    throws UnauthenticatedException, BadRequestException, NotFoundException, IOException, UnauthorizedException {
    return getMetadata(id, null);
  }

  /**
   * @param id the entity for which to retrieve metadata
   * @param scope the {@link MetadataScope} to retrieve the metadata from. If null, this method retrieves
   *              metadata from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata for the entity.
   */
  public Set<MetadataRecord> getMetadata(EntityId id, @Nullable MetadataScope scope)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof ApplicationId) {
      return getMetadata((ApplicationId) id, scope);
    } else if (id instanceof ArtifactId) {
      return getMetadata((ArtifactId) id, scope);
    } else if (id instanceof DatasetId) {
      return getMetadata((DatasetId) id, scope);
    } else if (id instanceof StreamId) {
      return getMetadata((StreamId) id, scope);
    } else if (id instanceof StreamViewId) {
      return getMetadata((StreamViewId) id, scope);
    } else if (id instanceof ProgramId) {
      return getMetadata((ProgramId) id, scope);
    } else if (id instanceof ProgramRunId) {
      return getMetadata((ProgramRunId) id);
    }

    throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
  }

  /**
   * @param id the entity for which to retrieve metadata properties across {@link MetadataScope#SYSTEM} and
   * {@link MetadataScope#USER}
   * @return The metadata properties for the entity.
   */
  public Map<String, String> getProperties(EntityId id)
    throws UnauthenticatedException, BadRequestException, NotFoundException, IOException, UnauthorizedException {
    return getProperties(id, null);
  }

  /**
   * @param id the entity for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the properties from. If null, this method retrieves
   *              properties from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the entity.
   */
  public Map<String, String> getProperties(EntityId id, @Nullable MetadataScope scope)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof ApplicationId) {
      return getProperties((ApplicationId) id, scope);
    } else if (id instanceof ArtifactId) {
      return getProperties((ArtifactId) id, scope);
    } else if (id instanceof DatasetId) {
      return getProperties((DatasetId) id, scope);
    } else if (id instanceof StreamId) {
      return getProperties((StreamId) id, scope);
    } else if (id instanceof StreamViewId) {
      return getProperties((StreamViewId) id, scope);
    } else if (id instanceof ProgramId) {
      return getProperties((ProgramId) id, scope);
    }

    throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
  }

  /**
   * @param id the entity for which to retrieve metadata tags across {@link MetadataScope#SYSTEM} and
   * {@link MetadataScope#USER}
   * @return The metadata tags for the entity.
   */
  public Set<String> getTags(EntityId id)
    throws UnauthenticatedException, BadRequestException, NotFoundException, IOException, UnauthorizedException {
    return getTags(id, null);
  }

  /**
   * @param id the entity for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If null, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the entity.
   */
  public Set<String> getTags(EntityId id, @Nullable MetadataScope scope)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof ApplicationId) {
      return getTags((ApplicationId) id, scope);
    } else if (id instanceof ArtifactId) {
      return getTags((ArtifactId) id, scope);
    } else if (id instanceof DatasetId) {
      return getTags((DatasetId) id, scope);
    } else if (id instanceof StreamId) {
      return getTags((StreamId) id, scope);
    } else if (id instanceof StreamViewId) {
      return getTags((StreamViewId) id, scope);
    } else if (id instanceof ProgramId) {
      return getTags((ProgramId) id, scope);
    }

    throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
  }

  /**
   * @param id the entity for which to add metadata properties
   * @param properties the metadata properties
   */
  public void addProperties(EntityId id, Map<String, String> properties)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof ApplicationId) {
      addProperties((ApplicationId) id, properties);
    } else if (id instanceof ArtifactId) {
      addProperties((ArtifactId) id, properties);
    } else if (id instanceof DatasetId) {
      addProperties((DatasetId) id, properties);
    } else if (id instanceof StreamId) {
      addProperties((StreamId) id, properties);
    } else if (id instanceof StreamViewId) {
      addProperties((StreamViewId) id, properties);
    } else if (id instanceof ProgramId) {
      addProperties((ProgramId) id, properties);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to add metadata tags
   * @param tags the metadata tags
   */
  public void addTags(EntityId id, Set<String> tags)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof ApplicationId) {
      addTags((ApplicationId) id, tags);
    } else if (id instanceof ArtifactId) {
      addTags((ArtifactId) id, tags);
    } else if (id instanceof DatasetId) {
      addTags((DatasetId) id, tags);
    } else if (id instanceof StreamId) {
      addTags((StreamId) id, tags);
    } else if (id instanceof StreamViewId) {
      addTags((StreamViewId) id, tags);
    } else if (id instanceof ProgramId) {
      addTags((ProgramId) id, tags);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to remove metadata
   */
  public void removeMetadata(EntityId id)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof ApplicationId) {
      removeMetadata((ApplicationId) id);
    } else if (id instanceof ArtifactId) {
      removeMetadata((ArtifactId) id);
    } else if (id instanceof DatasetId) {
      removeMetadata((DatasetId) id);
    } else if (id instanceof StreamId) {
      removeMetadata((StreamId) id);
    } else if (id instanceof StreamViewId) {
      removeMetadata((StreamViewId) id);
    } else if (id instanceof ProgramId) {
      removeMetadata((ProgramId) id);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to remove metadata properties
   */
  public void removeProperties(EntityId id)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof ApplicationId) {
      removeProperties((ApplicationId) id);
    } else if (id instanceof ArtifactId) {
      removeProperties((ArtifactId) id);
    } else if (id instanceof DatasetId) {
      removeProperties((DatasetId) id);
    } else if (id instanceof StreamId) {
      removeProperties((StreamId) id);
    } else if (id instanceof StreamViewId) {
      removeProperties((StreamViewId) id);
    } else if (id instanceof ProgramId) {
      removeProperties((ProgramId) id);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to remove metadata tags
   */
  public void removeTags(EntityId id)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof ApplicationId) {
      removeTags((ApplicationId) id);
    } else if (id instanceof ArtifactId) {
      removeTags((ArtifactId) id);
    } else if (id instanceof DatasetId) {
      removeTags((DatasetId) id);
    } else if (id instanceof StreamId) {
      removeTags((StreamId) id);
    } else if (id instanceof StreamViewId) {
      removeTags((StreamViewId) id);
    } else if (id instanceof ProgramId) {
      removeTags((ProgramId) id);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to remove a metadata property
   * @param property the property to remove
   */
  public void removeProperty(EntityId id, String property)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof ApplicationId) {
      removeProperty((ApplicationId) id, property);
    } else if (id instanceof ArtifactId) {
      removeProperty((ArtifactId) id, property);
    } else if (id instanceof DatasetId) {
      removeProperty((DatasetId) id, property);
    } else if (id instanceof StreamId) {
      removeProperty((StreamId) id, property);
    } else if (id instanceof StreamViewId) {
      removeProperty((StreamViewId) id, property);
    } else if (id instanceof ProgramId) {
      removeProperty((ProgramId) id, property);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to remove a metadata tag
   * @param tag the tag to remove
   */
  public void removeTag(EntityId id, String tag)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof ApplicationId) {
      removeTag((ApplicationId) id, tag);
    } else if (id instanceof ArtifactId) {
      removeTag((ArtifactId) id, tag);
    } else if (id instanceof DatasetId) {
      removeTag((DatasetId) id, tag);
    } else if (id instanceof StreamId) {
      removeTag((StreamId) id, tag);
    } else if (id instanceof StreamViewId) {
      removeTag((StreamViewId) id, tag);
    } else if (id instanceof ProgramId) {
      removeTag((ProgramId) id, tag);
    } else if (id instanceof ProgramRunId) {
      removeTag((ProgramRunId) id, tag);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param appId the app for which to retrieve metadata
   * @return The metadata for the application.
   */
  public Set<MetadataRecord> getMetadata(ApplicationId appId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(appId, constructPath(appId), scope);
  }

  /**
   * @param artifactId the artifact for which to retrieve metadata
   * @return The metadata for the artifact.
   */
  public Set<MetadataRecord> getMetadata(ArtifactId artifactId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(artifactId, constructPath(artifactId), scope);
  }

  /**
   * @param datasetInstance the dataset for which to retrieve metadata
   * @return The metadata for the dataset.
   */
  public Set<MetadataRecord> getMetadata(DatasetId datasetInstance, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(datasetInstance, constructPath(datasetInstance), scope);
  }

  /**
   * @param streamId the stream for which to retrieve metadata
   * @return The metadata for the stream.
   */
  public Set<MetadataRecord> getMetadata(StreamId streamId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(streamId, constructPath(streamId), scope);
  }

  /**
   * @param viewId the view for which to retrieve metadata
   * @return The metadata for the view.
   */
  public Set<MetadataRecord> getMetadata(StreamViewId viewId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(viewId, constructPath(viewId), scope);
  }

  /**
   * @param programId the program for which to retrieve metadata
   * @return The metadata for the program.
   */
  public Set<MetadataRecord> getMetadata(ProgramId programId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(programId, constructPath(programId), scope);
  }

  /**
   * @param runId the run for which to retrieve metadata
   * @return The metadata for the run.
   */
  public Set<MetadataRecord> getMetadata(ProgramRunId runId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(runId, constructPath(runId));
  }

  private Set<MetadataRecord> doGetMetadata(NamespacedEntityId namespacedId, String entityPath)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    return doGetMetadata(namespacedId, entityPath, null);
  }

  private Set<MetadataRecord> doGetMetadata(NamespacedEntityId namespacedId, String entityPath,
                                            @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata", entityPath);
    path = scope == null ? path : String.format("%s?scope=%s", path, scope);
    HttpResponse response = makeRequest(namespacedId, path, HttpMethod.GET);
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_RECORD_TYPE);
  }




  /**
   * @param appId the app for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the application.
   */
  public Map<String, String> getProperties(ApplicationId appId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(appId, constructPath(appId), scope);
  }

  /**
   * @param artifactId the artifact for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the artifact.
   */
  public Map<String, String> getProperties(ArtifactId artifactId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(artifactId, constructPath(artifactId), scope);
  }

  /**
   * @param datasetInstance the dataset for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the dataset.
   */
  public Map<String, String> getProperties(DatasetId datasetInstance, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(datasetInstance, constructPath(datasetInstance), scope);
  }

  /**
   * @param streamId the stream for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the stream.
   */
  public Map<String, String> getProperties(StreamId streamId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(streamId, constructPath(streamId), scope);
  }

  /**
   * @param viewId the view for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the view.
   */
  public Map<String, String> getProperties(StreamViewId viewId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(viewId, constructPath(viewId), scope);
  }

  /**
   * @param programId the program for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the program.
   */
  public Map<String, String> getProperties(ProgramId programId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(programId, constructPath(programId), scope);
  }

  private Map<String, String> getProperties(NamespacedEntityId namespacedId, String entityPath,
                                            @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/properties", entityPath);
    path = scope == null ? path : String.format("%s?scope=%s", path, scope);
    HttpResponse response = makeRequest(namespacedId, path, HttpMethod.GET);
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  /**
   * @param appId the app for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the application.
   */
  public Set<String> getTags(ApplicationId appId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(appId, constructPath(appId), scope);
  }

  /**
   * @param artifactId the artifact for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the artifact.
   */
  public Set<String> getTags(ArtifactId artifactId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(artifactId, constructPath(artifactId), scope);
  }

  /**
   * @param datasetInstance the dataset for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the dataset.
   */
  public Set<String> getTags(DatasetId datasetInstance, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(datasetInstance, constructPath(datasetInstance), scope);
  }

  /**
   * @param streamId the stream for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the stream.
   */
  public Set<String> getTags(StreamId streamId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(streamId, constructPath(streamId), scope);
  }

  /**
   * @param viewId the view for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the view.
   */
  public Set<String> getTags(StreamViewId viewId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(viewId, constructPath(viewId), scope);
  }

  /**
   * @param programId the program for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the program.
   */
  public Set<String> getTags(ProgramId programId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(programId, constructPath(programId), scope);
  }

  private Set<String> getTags(NamespacedEntityId namespacedId, String entityPath, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/tags", entityPath);
    path = scope == null ? path : String.format("%s?scope=%s", path, scope);
    HttpResponse response = makeRequest(namespacedId, path, HttpMethod.GET);
    return GSON.fromJson(response.getResponseBodyAsString(), SET_STRING_TYPE);
  }

  /**
   * Adds tags to an application.
   *
   * @param appId app to add tags to
   * @param tags tags to be added
   */
  public void addTags(ApplicationId appId, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(appId, constructPath(appId), tags);
  }

  /**
   * Adds tags to an artifact.
   *
   * @param artifactId artifact to add tags to
   * @param tags tags to be added
   */
  public void addTags(ArtifactId artifactId, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(artifactId, constructPath(artifactId), tags);
  }

  /**
   * Adds tags to a dataset.
   *
   * @param datasetInstance dataset to add tags to
   * @param tags tags to be added
   */
  public void addTags(DatasetId datasetInstance, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(datasetInstance, constructPath(datasetInstance), tags);
  }

  /**
   * Adds tags to a stream.
   *
   * @param streamId stream to add tags to
   * @param tags tags to be added
   */
  public void addTags(StreamId streamId, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(streamId, constructPath(streamId), tags);
  }

  /**
   * Adds tags to a view.
   *
   * @param viewId view to add tags to
   * @param tags tags to be added
   */
  public void addTags(StreamViewId viewId, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(viewId, constructPath(viewId), tags);
  }

  /**
   * Adds tags to a program.
   *
   * @param programId program to add tags to
   * @param tags tags to be added
   */
  public void addTags(ProgramId programId, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(programId, constructPath(programId), tags);
  }

  private void addTags(NamespacedEntityId namespacedId, String entityPath, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/tags", entityPath);
    makeRequest(namespacedId, path, HttpMethod.POST, GSON.toJson(tags));
  }

  /**
   * Adds properties to an application.
   *
   * @param appId app to add properties to
   * @param properties properties to be added
   */
  public void addProperties(ApplicationId appId, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(appId, constructPath(appId), properties);
  }

  /**
   * Adds properties to an artifact.
   *
   * @param artifactId artifact to add properties to
   * @param properties properties to be added
   */
  public void addProperties(ArtifactId artifactId, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(artifactId, constructPath(artifactId), properties);
  }

  /**
   * Adds properties to a dataset.
   *
   * @param datasetInstance dataset to add properties to
   * @param properties properties to be added
   */
  public void addProperties(DatasetId datasetInstance, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(datasetInstance, constructPath(datasetInstance), properties);
  }

  /**
   * Adds properties to a stream.
   *
   * @param streamId stream to add properties to
   * @param properties properties to be added
   */
  public void addProperties(StreamId streamId, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(streamId, constructPath(streamId), properties);
  }

  /**
   * Adds properties to a view.
   *
   * @param viewId view to add properties to
   * @param properties properties to be added
   */
  public void addProperties(StreamViewId viewId, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(viewId, constructPath(viewId), properties);
  }

  /**
   * Adds properties to a program.
   *
   * @param programId program to add properties to
   * @param properties properties to be added
   */
  public void addProperties(ProgramId programId, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(programId, constructPath(programId), properties);
  }

  private void addProperties(NamespacedEntityId namespacedId, String entityPath, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/properties", entityPath);
    makeRequest(namespacedId, path, HttpMethod.POST, GSON.toJson(properties));
  }

  /**
   * Removes a property from an application.
   *
   * @param appId app to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(ApplicationId appId, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(appId, constructPath(appId), propertyToRemove);
  }

  /**
   * Removes a property from an artifact.
   *
   * @param artifactId artifact to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(ArtifactId artifactId, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(artifactId, constructPath(artifactId), propertyToRemove);
  }

  /**
   * Removes a property from a dataset.
   *
   * @param datasetInstance dataset to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(DatasetId datasetInstance, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(datasetInstance, constructPath(datasetInstance), propertyToRemove);
  }

  /**
   * Removes a property from a stream.
   *
   * @param streamId stream to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(StreamId streamId, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(streamId, constructPath(streamId), propertyToRemove);
  }

  /**
   * Removes a property from a view.
   *
   * @param viewId view to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(StreamViewId viewId, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(viewId, constructPath(viewId), propertyToRemove);
  }

  /**
   * Removes a property from a program.
   *
   * @param programId program to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(ProgramId programId, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(programId, constructPath(programId), propertyToRemove);
  }

  private void removeProperty(NamespacedEntityId namespacedId, String entityPath, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/properties/%s", entityPath, propertyToRemove);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }


  /**
   * Removes all properties from an application.
   *
   * @param appId app to remove properties from
   */
  public void removeProperties(ApplicationId appId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(appId, constructPath(appId));
  }

  /**
   * Removes all properties from an artifact.
   *
   * @param artifactId artifact to remove properties from
   */
  public void removeProperties(ArtifactId artifactId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(artifactId, constructPath(artifactId));
  }

  /**
   * Removes all properties from a dataset.
   *
   * @param datasetInstance dataset to remove properties from
   */
  public void removeProperties(DatasetId datasetInstance)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(datasetInstance, constructPath(datasetInstance));
  }

  /**
   * Removes all properties from a stream.
   *
   * @param streamId stream to remove properties from
   */
  public void removeProperties(StreamId streamId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(streamId, constructPath(streamId));
  }

  /**
   * Removes all properties from a view.
   *
   * @param viewId view to remove properties from
   */
  public void removeProperties(StreamViewId viewId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(viewId, constructPath(viewId));
  }

  /**
   * Removes all properties from a program.
   *
   * @param programId program to remove properties from
   */
  public void removeProperties(ProgramId programId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(programId, constructPath(programId));
  }

  private void removeProperties(NamespacedEntityId namespacedId, String entityPath)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/properties", entityPath);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

  /**
   * Removes a tag from an application.
   *
   * @param appId app to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(ApplicationId appId, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(appId, constructPath(appId), tagToRemove);
  }

  /**
   * Removes a tag from an artifact.
   *
   * @param artifactId artifact to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(ArtifactId artifactId, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(artifactId, constructPath(artifactId), tagToRemove);
  }


  /**
   * Removes a tag from a dataset.
   *
   * @param datasetInstance dataset to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(DatasetId datasetInstance, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(datasetInstance, constructPath(datasetInstance), tagToRemove);
  }

  /**
   * Removes a tag from a stream.
   *
   * @param streamId stream to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(StreamId streamId, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(streamId, constructPath(streamId), tagToRemove);
  }

  /**
   * Removes a tag from a view.
   *
   * @param viewId view to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(StreamViewId viewId, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(viewId, constructPath(viewId), tagToRemove);
  }

  /**
   * Removes a tag from a program.
   *
   * @param programId program to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(ProgramId programId, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(programId, constructPath(programId), tagToRemove);
  }

  private void removeTag(NamespacedEntityId namespacedId, String entityPath, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/tags/%s", entityPath, tagToRemove);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

  /**
   * Removes all tags from an application.
   *
   * @param appId app to remove tags from
   */
  public void removeTags(ApplicationId appId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(appId, constructPath(appId));
  }

  /**
   * Removes all tags from an artifact.
   *
   * @param artifactId artifact to remove tags from
   */
  public void removeTags(ArtifactId artifactId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(artifactId, constructPath(artifactId));
  }

  /**
   * Removes all tags from a dataset.
   *
   * @param datasetInstance dataset to remove tags from
   */
  public void removeTags(DatasetId datasetInstance)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(datasetInstance, constructPath(datasetInstance));
  }

  /**
   * Removes all tags from a stream.
   *
   * @param streamId stream to remove tags from
   */
  public void removeTags(StreamId streamId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(streamId, constructPath(streamId));
  }

  /**
   * Removes all tags from a view.
   *
   * @param viewId view to remove tags from
   */
  public void removeTags(StreamViewId viewId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(viewId, constructPath(viewId));
  }

  /**
   * Removes all tags from a program.
   *
   * @param programId program to remove tags from
   */
  public void removeTags(ProgramId programId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(programId, constructPath(programId));
  }

  private void removeTags(NamespacedEntityId namespacedId, String entityPath)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/tags", entityPath);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

  /**
   * Removes metadata from an application.
   *
   * @param appId app to remove metadata from
   */
  public void removeMetadata(ApplicationId appId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(appId, constructPath(appId));
  }

  /**
   * Removes metadata from an artifact.
   *
   * @param artifactId artifact to remove metadata from
   */
  public void removeMetadata(ArtifactId artifactId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(artifactId, constructPath(artifactId));
  }

  /**
   * Removes metadata from a dataset.
   *
   * @param datasetInstance dataset to remove metadata from
   */
  public void removeMetadata(DatasetId datasetInstance)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(datasetInstance, constructPath(datasetInstance));
  }

  /**
   * Removes metadata from a stream.
   *
   * @param streamId stream to remove metadata from
   */
  public void removeMetadata(StreamId streamId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(streamId, constructPath(streamId));
  }

  /**
   * Removes metadata from a view.
   *
   * @param viewId view to remove metadata from
   */
  public void removeMetadata(StreamViewId viewId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(viewId, constructPath(viewId));
  }

  /**
   * Removes metadata from a program.
   *
   * @param programId program to remove metadata from
   */
  public void removeMetadata(ProgramId programId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(programId, constructPath(programId));
  }

  private void removeMetadata(NamespacedEntityId namespacedId, String entityPath)
    throws UnauthenticatedException, BadRequestException, NotFoundException, IOException, UnauthorizedException {
    String path = String.format("%s/metadata", entityPath);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

  private HttpResponse makeRequest(NamespacedEntityId namespacedId, String path, HttpMethod httpMethod)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    return makeRequest(namespacedId, path, httpMethod, null);
  }

  // makes a request and throws BadRequestException or NotFoundException, as appropriate
  private HttpResponse makeRequest(NamespacedEntityId namespacedId, String path,
                                   HttpMethod httpMethod, @Nullable String body)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    URL url = resolve(new NamespaceId(namespacedId.getNamespace()), path);
    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url);
    if (body != null) {
      builder.withBody(body);
    }
    HttpResponse response = execute(builder.build(),
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
  private String constructPath(ApplicationId appId) {
    return String.format("apps/%s", appId.getApplication());
  }

  private String constructPath(ArtifactId artifactId) {
    return String.format("artifacts/%s/versions/%s", artifactId.getArtifact(), artifactId.getVersion());
  }

  private String constructPath(ProgramId programId) {
    return String.format("apps/%s/%s/%s",
                         programId.getApplication(), programId.getType().getCategoryName(), programId.getProgram());
  }

  private String constructPath(ProgramRunId runId) {
    ProgramId programId = runId.getParent();
    return String.format("apps/%s/%s/%s/runs/%s",
                         programId.getApplication(), programId.getType().getCategoryName(), programId.getProgram(),
                         runId.getRun());
  }

  private String constructPath(DatasetId datasetInstance) {
    return String.format("datasets/%s", datasetInstance.getDataset());
  }

  private String constructPath(StreamId streamId) {
    return String.format("streams/%s", streamId.getStream());
  }

  private String constructPath(StreamViewId viewId) {
    return String.format("streams/%s/views/%s", viewId.getStream(), viewId.getView());
  }
}

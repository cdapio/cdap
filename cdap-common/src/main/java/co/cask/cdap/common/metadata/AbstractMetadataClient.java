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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.id.NamespacedEntityId;
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
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
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
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
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
  protected abstract URL resolve(Id.Namespace namesapace, String resource) throws IOException;

  /**
   * Searches entities in the specified namespace whose metadata matches the specified query.
   *
   * @param namespace the namespace to search in
   * @param query the query string with which to search
   * @param target the target type. If null, all possible types will be searched
   * @return the {@link MetadataSearchResponse} for the given query.
   */
  public MetadataSearchResponse searchMetadata(Id.Namespace namespace, String query,
                                               @Nullable MetadataSearchTargetType target)
    throws IOException, UnauthenticatedException, UnauthorizedException, BadRequestException {
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
  public MetadataSearchResponse searchMetadata(Id.Namespace namespace, String query,
                                               Set<MetadataSearchTargetType> targets)
    throws IOException, UnauthenticatedException, UnauthorizedException, BadRequestException {
    return searchMetadata(namespace, query, targets, null, 0, Integer.MAX_VALUE, 0, null, false);
  }

  /**
   * Searches entities in the specified namespace whose metadata matches the specified query.
   *
   * @param namespace the namespace to search in
   * @param query the query string with which to search
   * @param targets {@link MetadataSearchTargetType}s to search. If empty, all possible types will be searched
   * @param sort specifies sort field and sort order. If {@code null}, the sort order is by relevance
   * @param offset the index to start with in the search results. To return results from the beginning, pass {@code 0}
   * @param limit the number of results to return, starting from #offset. To return all, pass {@link Integer#MAX_VALUE}
   * @param numCursors the number of cursors to return in the response. A cursor identifies the first index of the
   *                   next page for pagination purposes
   * @param cursor the cursor that acts as the starting index for the requested page. This is only applicable when
   *               #sortInfo is not default. If offset is also specified, it is applied starting at
   *               the cursor. If {@code null}, the first row is used as the cursor
   * @param showHidden boolean which specifies whether to display hidden entities (entity whose name start with "_")
   *                    or not.
   * @return A set of {@link MetadataSearchResultRecord} for the given query.
   */
  public MetadataSearchResponse searchMetadata(Id.Namespace namespace, String query,
                                               Set<MetadataSearchTargetType> targets, @Nullable String sort,
                                               int offset, int limit, int numCursors,
                                               @Nullable String cursor, boolean showHidden)
    throws IOException, UnauthenticatedException, UnauthorizedException, BadRequestException {

    String path = String.format("metadata/search?query=%s", query);
    for (MetadataSearchTargetType t : targets) {
      path += "&target=" + t;
    }
    if (sort != null) {
      path += "&sort=" + URLEncoder.encode(sort, "UTF-8");
    }
    path += "&offset=" + offset;
    path += "&limit=" + limit;
    path += "&numCursors=" + numCursors;
    if (cursor != null) {
      path += "&cursor=" + cursor;
    }
    if (showHidden) {
      path += "&showHidden=" + true;
    }
    URL searchURL = resolve(namespace, path);
    HttpResponse response = execute(HttpRequest.get(searchURL).build(), HttpResponseStatus.BAD_REQUEST.getCode());
    if (HttpResponseStatus.BAD_REQUEST.getCode() == response.getResponseCode()) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
    return GSON.fromJson(response.getResponseBodyAsString(), MetadataSearchResponse.class);
  }

  /**
   * @param id the entity for which to retrieve metadata across {@link MetadataScope#SYSTEM} and
   * {@link MetadataScope#USER}
   * @return The metadata for the entity.
   */
  public Set<MetadataRecord> getMetadata(Id id)
    throws UnauthenticatedException, BadRequestException, NotFoundException, IOException, UnauthorizedException {
    return getMetadata(id, null);
  }

  /**
   * @param id the entity for which to retrieve metadata
   * @param scope the {@link MetadataScope} to retrieve the metadata from. If null, this method retrieves
   *              metadata from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata for the entity.
   */
  public Set<MetadataRecord> getMetadata(Id id, @Nullable MetadataScope scope)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof Id.Application) {
      return getMetadata((Id.Application) id, scope);
    } else if (id instanceof Id.Artifact) {
      return getMetadata((Id.Artifact) id, scope);
    } else if (id instanceof Id.DatasetInstance) {
      return getMetadata((Id.DatasetInstance) id, scope);
    } else if (id instanceof Id.Stream) {
      return getMetadata((Id.Stream) id, scope);
    } else if (id instanceof Id.Stream.View) {
      return getMetadata((Id.Stream.View) id, scope);
    } else if (id instanceof Id.Program) {
      return getMetadata((Id.Program) id, scope);
    } else if (id instanceof Id.Run) {
      return getMetadata((Id.Run) id);
    }

    throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
  }

  /**
   * @param id the entity for which to retrieve metadata properties across {@link MetadataScope#SYSTEM} and
   * {@link MetadataScope#USER}
   * @return The metadata properties for the entity.
   */
  public Map<String, String> getProperties(Id id)
    throws UnauthenticatedException, BadRequestException, NotFoundException, IOException, UnauthorizedException {
    return getProperties(id, null);
  }

  /**
   * @param id the entity for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the properties from. If null, this method retrieves
   *              properties from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the entity.
   */
  public Map<String, String> getProperties(Id id, @Nullable MetadataScope scope)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof Id.Application) {
      return getProperties((Id.Application) id, scope);
    } else if (id instanceof Id.Artifact) {
      return getProperties((Id.Artifact) id, scope);
    } else if (id instanceof Id.DatasetInstance) {
      return getProperties((Id.DatasetInstance) id, scope);
    } else if (id instanceof Id.Stream) {
      return getProperties((Id.Stream) id, scope);
    } else if (id instanceof Id.Stream.View) {
      return getProperties((Id.Stream.View) id, scope);
    } else if (id instanceof Id.Program) {
      return getProperties((Id.Program) id, scope);
    }

    throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
  }

  /**
   * @param id the entity for which to retrieve metadata tags across {@link MetadataScope#SYSTEM} and
   * {@link MetadataScope#USER}
   * @return The metadata tags for the entity.
   */
  public Set<String> getTags(Id id)
    throws UnauthenticatedException, BadRequestException, NotFoundException, IOException, UnauthorizedException {
    return getTags(id, null);
  }

  /**
   * @param id the entity for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If null, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the entity.
   */
  public Set<String> getTags(Id id, @Nullable MetadataScope scope)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof Id.Application) {
      return getTags((Id.Application) id, scope);
    } else if (id instanceof Id.Artifact) {
      return getTags((Id.Artifact) id, scope);
    } else if (id instanceof Id.DatasetInstance) {
      return getTags((Id.DatasetInstance) id, scope);
    } else if (id instanceof Id.Stream) {
      return getTags((Id.Stream) id, scope);
    } else if (id instanceof Id.Stream.View) {
      return getTags((Id.Stream.View) id, scope);
    } else if (id instanceof Id.Program) {
      return getTags((Id.Program) id, scope);
    }

    throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
  }

  /**
   * @param id the entity for which to add metadata properties
   * @param properties the metadata properties
   */
  public void addProperties(Id id, Map<String, String> properties)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof Id.Application) {
      addProperties((Id.Application) id, properties);
    } else if (id instanceof Id.Artifact) {
      addProperties((Id.Artifact) id, properties);
    } else if (id instanceof Id.DatasetInstance) {
      addProperties((Id.DatasetInstance) id, properties);
    } else if (id instanceof Id.Stream) {
      addProperties((Id.Stream) id, properties);
    } else if (id instanceof Id.Stream.View) {
      addProperties((Id.Stream.View) id, properties);
    } else if (id instanceof Id.Program) {
      addProperties((Id.Program) id, properties);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to add metadata tags
   * @param tags the metadata tags
   */
  public void addTags(Id id, Set<String> tags)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof Id.Application) {
      addTags((Id.Application) id, tags);
    } else if (id instanceof Id.Artifact) {
      addTags((Id.Artifact) id, tags);
    } else if (id instanceof Id.DatasetInstance) {
      addTags((Id.DatasetInstance) id, tags);
    } else if (id instanceof Id.Stream) {
      addTags((Id.Stream) id, tags);
    } else if (id instanceof Id.Stream.View) {
      addTags((Id.Stream.View) id, tags);
    } else if (id instanceof Id.Program) {
      addTags((Id.Program) id, tags);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to remove metadata
   */
  public void removeMetadata(Id id)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof Id.Application) {
      removeMetadata((Id.Application) id);
    } else if (id instanceof Id.Artifact) {
      removeMetadata((Id.Artifact) id);
    } else if (id instanceof Id.DatasetInstance) {
      removeMetadata((Id.DatasetInstance) id);
    } else if (id instanceof Id.Stream) {
      removeMetadata((Id.Stream) id);
    } else if (id instanceof Id.Stream.View) {
      removeMetadata((Id.Stream.View) id);
    } else if (id instanceof Id.Program) {
      removeMetadata((Id.Program) id);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to remove metadata properties
   */
  public void removeProperties(Id id)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof Id.Application) {
      removeProperties((Id.Application) id);
    } else if (id instanceof Id.Artifact) {
      removeProperties((Id.Artifact) id);
    } else if (id instanceof Id.DatasetInstance) {
      removeProperties((Id.DatasetInstance) id);
    } else if (id instanceof Id.Stream) {
      removeProperties((Id.Stream) id);
    } else if (id instanceof Id.Stream.View) {
      removeProperties((Id.Stream.View) id);
    } else if (id instanceof Id.Program) {
      removeProperties((Id.Program) id);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to remove metadata tags
   */
  public void removeTags(Id id)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof Id.Application) {
      removeTags((Id.Application) id);
    } else if (id instanceof Id.Artifact) {
      removeTags((Id.Artifact) id);
    } else if (id instanceof Id.DatasetInstance) {
      removeTags((Id.DatasetInstance) id);
    } else if (id instanceof Id.Stream) {
      removeTags((Id.Stream) id);
    } else if (id instanceof Id.Stream.View) {
      removeTags((Id.Stream.View) id);
    } else if (id instanceof Id.Program) {
      removeTags((Id.Program) id);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to remove a metadata property
   * @param property the property to remove
   */
  public void removeProperty(Id id, String property)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof Id.Application) {
      removeProperty((Id.Application) id, property);
    } else if (id instanceof Id.Artifact) {
      removeProperty((Id.Artifact) id, property);
    } else if (id instanceof Id.DatasetInstance) {
      removeProperty((Id.DatasetInstance) id, property);
    } else if (id instanceof Id.Stream) {
      removeProperty((Id.Stream) id, property);
    } else if (id instanceof Id.Stream.View) {
      removeProperty((Id.Stream.View) id, property);
    } else if (id instanceof Id.Program) {
      removeProperty((Id.Program) id, property);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param id the entity for which to remove a metadata tag
   * @param tag the tag to remove
   */
  public void removeTag(Id id, String tag)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {

    if (id instanceof Id.Application) {
      removeTag((Id.Application) id, tag);
    } else if (id instanceof Id.Artifact) {
      removeTag((Id.Artifact) id, tag);
    } else if (id instanceof Id.DatasetInstance) {
      removeTag((Id.DatasetInstance) id, tag);
    } else if (id instanceof Id.Stream) {
      removeTag((Id.Stream) id, tag);
    } else if (id instanceof Id.Stream.View) {
      removeTag((Id.Stream.View) id, tag);
    } else if (id instanceof Id.Program) {
      removeTag((Id.Program) id, tag);
    } else if (id instanceof Id.Run) {
      removeTag((Id.Run) id, tag);
    } else {
      throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
    }
  }

  /**
   * @param appId the app for which to retrieve metadata
   * @return The metadata for the application.
   */
  public Set<MetadataRecord> getMetadata(Id.Application appId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(appId, constructPath(appId), scope);
  }

  /**
   * @param artifactId the artifact for which to retrieve metadata
   * @return The metadata for the artifact.
   */
  public Set<MetadataRecord> getMetadata(Id.Artifact artifactId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(artifactId, constructPath(artifactId), scope);
  }

  /**
   * @param datasetInstance the dataset for which to retrieve metadata
   * @return The metadata for the dataset.
   */
  public Set<MetadataRecord> getMetadata(Id.DatasetInstance datasetInstance, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(datasetInstance, constructPath(datasetInstance), scope);
  }

  /**
   * @param streamId the stream for which to retrieve metadata
   * @return The metadata for the stream.
   */
  public Set<MetadataRecord> getMetadata(Id.Stream streamId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(streamId, constructPath(streamId), scope);
  }

  /**
   * @param viewId the view for which to retrieve metadata
   * @return The metadata for the view.
   */
  public Set<MetadataRecord> getMetadata(Id.Stream.View viewId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(viewId, constructPath(viewId), scope);
  }

  /**
   * @param programId the program for which to retrieve metadata
   * @return The metadata for the program.
   */
  public Set<MetadataRecord> getMetadata(Id.Program programId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(programId, constructPath(programId), scope);
  }

  /**
   * @param runId the run for which to retrieve metadata
   * @return The metadata for the run.
   */
  public Set<MetadataRecord> getMetadata(Id.Run runId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return doGetMetadata(runId, constructPath(runId));
  }

  private Set<MetadataRecord> doGetMetadata(Id.NamespacedId namespacedId, String entityPath)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    return doGetMetadata(namespacedId, entityPath, null);
  }

  private Set<MetadataRecord> doGetMetadata(Id.NamespacedId namespacedId, String entityPath,
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
  public Map<String, String> getProperties(Id.Application appId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(appId, constructPath(appId), scope);
  }

  /**
   * @param artifactId the artifact for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the artifact.
   */
  public Map<String, String> getProperties(Id.Artifact artifactId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(artifactId, constructPath(artifactId), scope);
  }

  /**
   * @param datasetInstance the dataset for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the dataset.
   */
  public Map<String, String> getProperties(Id.DatasetInstance datasetInstance, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(datasetInstance, constructPath(datasetInstance), scope);
  }

  /**
   * @param streamId the stream for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the stream.
   */
  public Map<String, String> getProperties(Id.Stream streamId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(streamId, constructPath(streamId), scope);
  }

  /**
   * @param viewId the view for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the view.
   */
  public Map<String, String> getProperties(Id.Stream.View viewId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(viewId, constructPath(viewId), scope);
  }

  /**
   * @param programId the program for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the program.
   */
  public Map<String, String> getProperties(Id.Program programId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getProperties(programId, constructPath(programId), scope);
  }

  private Map<String, String> getProperties(Id.NamespacedId namespacedId, String entityPath,
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
  public Set<String> getTags(Id.Application appId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(appId, constructPath(appId), scope);
  }

  /**
   * @param artifactId the artifact for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the artifact.
   */
  public Set<String> getTags(Id.Artifact artifactId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(artifactId, constructPath(artifactId), scope);
  }

  /**
   * @param datasetInstance the dataset for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the dataset.
   */
  public Set<String> getTags(Id.DatasetInstance datasetInstance, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(datasetInstance, constructPath(datasetInstance), scope);
  }

  /**
   * @param streamId the stream for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the stream.
   */
  public Set<String> getTags(Id.Stream streamId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(streamId, constructPath(streamId), scope);
  }

  /**
   * @param viewId the view for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the view.
   */
  public Set<String> getTags(Id.Stream.View viewId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(viewId, constructPath(viewId), scope);
  }

  /**
   * @param programId the program for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the program.
   */
  public Set<String> getTags(Id.Program programId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    return getTags(programId, constructPath(programId), scope);
  }

  private Set<String> getTags(Id.NamespacedId namespacedId, String entityPath, @Nullable MetadataScope scope)
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
  public void addTags(Id.Application appId, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(appId, constructPath(appId), tags);
  }

  /**
   * Adds tags to an artifact.
   *
   * @param artifactId artifact to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Artifact artifactId, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(artifactId, constructPath(artifactId), tags);
  }

  /**
   * Adds tags to a dataset.
   *
   * @param datasetInstance dataset to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.DatasetInstance datasetInstance, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(datasetInstance, constructPath(datasetInstance), tags);
  }

  /**
   * Adds tags to a stream.
   *
   * @param streamId stream to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Stream streamId, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(streamId, constructPath(streamId), tags);
  }

  /**
   * Adds tags to a view.
   *
   * @param viewId view to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Stream.View viewId, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(viewId, constructPath(viewId), tags);
  }

  /**
   * Adds tags to a program.
   *
   * @param programId program to add tags to
   * @param tags tags to be added
   */
  public void addTags(Id.Program programId, Set<String> tags)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addTags(programId, constructPath(programId), tags);
  }

  private void addTags(Id.NamespacedId namespacedId, String entityPath, Set<String> tags)
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
  public void addProperties(Id.Application appId, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(appId, constructPath(appId), properties);
  }

  /**
   * Adds properties to an artifact.
   *
   * @param artifactId artifact to add properties to
   * @param properties properties to be added
   */
  public void addProperties(Id.Artifact artifactId, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(artifactId, constructPath(artifactId), properties);
  }

  /**
   * Adds properties to a dataset.
   *
   * @param datasetInstance dataset to add properties to
   * @param properties properties to be added
   */
  public void addProperties(Id.DatasetInstance datasetInstance, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(datasetInstance, constructPath(datasetInstance), properties);
  }

  /**
   * Adds properties to a stream.
   *
   * @param streamId stream to add properties to
   * @param properties properties to be added
   */
  public void addProperties(Id.Stream streamId, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(streamId, constructPath(streamId), properties);
  }

  /**
   * Adds properties to a view.
   *
   * @param viewId view to add properties to
   * @param properties properties to be added
   */
  public void addProperties(Id.Stream.View viewId, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(viewId, constructPath(viewId), properties);
  }

  /**
   * Adds properties to a program.
   *
   * @param programId program to add properties to
   * @param properties properties to be added
   */
  public void addProperties(Id.Program programId, Map<String, String> properties)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    addProperties(programId, constructPath(programId), properties);
  }

  private void addProperties(Id.NamespacedId namespacedId, String entityPath, Map<String, String> properties)
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
  public void removeProperty(Id.Application appId, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(appId, constructPath(appId), propertyToRemove);
  }

  /**
   * Removes a property from an artifact.
   *
   * @param artifactId artifact to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(Id.Artifact artifactId, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(artifactId, constructPath(artifactId), propertyToRemove);
  }

  /**
   * Removes a property from a dataset.
   *
   * @param datasetInstance dataset to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(Id.DatasetInstance datasetInstance, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(datasetInstance, constructPath(datasetInstance), propertyToRemove);
  }

  /**
   * Removes a property from a stream.
   *
   * @param streamId stream to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(Id.Stream streamId, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(streamId, constructPath(streamId), propertyToRemove);
  }

  /**
   * Removes a property from a view.
   *
   * @param viewId view to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(Id.Stream.View viewId, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(viewId, constructPath(viewId), propertyToRemove);
  }

  /**
   * Removes a property from a program.
   *
   * @param programId program to remove property from
   * @param propertyToRemove property to be removed
   */
  public void removeProperty(Id.Program programId, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperty(programId, constructPath(programId), propertyToRemove);
  }

  private void removeProperty(Id.NamespacedId namespacedId, String entityPath, String propertyToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/properties/%s", entityPath, propertyToRemove);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }


  /**
   * Removes all properties from an application.
   *
   * @param appId app to remove properties from
   */
  public void removeProperties(Id.Application appId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(appId, constructPath(appId));
  }

  /**
   * Removes all properties from an artifact.
   *
   * @param artifactId artifact to remove properties from
   */
  public void removeProperties(Id.Artifact artifactId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(artifactId, constructPath(artifactId));
  }

  /**
   * Removes all properties from a dataset.
   *
   * @param datasetInstance dataset to remove properties from
   */
  public void removeProperties(Id.DatasetInstance datasetInstance)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(datasetInstance, constructPath(datasetInstance));
  }

  /**
   * Removes all properties from a stream.
   *
   * @param streamId stream to remove properties from
   */
  public void removeProperties(Id.Stream streamId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(streamId, constructPath(streamId));
  }

  /**
   * Removes all properties from a view.
   *
   * @param viewId view to remove properties from
   */
  public void removeProperties(Id.Stream.View viewId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(viewId, constructPath(viewId));
  }

  /**
   * Removes all properties from a program.
   *
   * @param programId program to remove properties from
   */
  public void removeProperties(Id.Program programId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeProperties(programId, constructPath(programId));
  }

  private void removeProperties(Id.NamespacedId namespacedId, String entityPath)
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
  public void removeTag(Id.Application appId, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(appId, constructPath(appId), tagToRemove);
  }

  /**
   * Removes a tag from an artifact.
   *
   * @param artifactId artifact to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(Id.Artifact artifactId, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(artifactId, constructPath(artifactId), tagToRemove);
  }


  /**
   * Removes a tag from a dataset.
   *
   * @param datasetInstance dataset to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(Id.DatasetInstance datasetInstance, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(datasetInstance, constructPath(datasetInstance), tagToRemove);
  }

  /**
   * Removes a tag from a stream.
   *
   * @param streamId stream to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(Id.Stream streamId, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(streamId, constructPath(streamId), tagToRemove);
  }

  /**
   * Removes a tag from a view.
   *
   * @param viewId view to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(Id.Stream.View viewId, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(viewId, constructPath(viewId), tagToRemove);
  }

  /**
   * Removes a tag from a program.
   *
   * @param programId program to remove tag from
   * @param tagToRemove tag to be removed
   */
  public void removeTag(Id.Program programId, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTag(programId, constructPath(programId), tagToRemove);
  }

  private void removeTag(Id.NamespacedId namespacedId, String entityPath, String tagToRemove)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/tags/%s", entityPath, tagToRemove);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

  /**
   * Removes all tags from an application.
   *
   * @param appId app to remove tags from
   */
  public void removeTags(Id.Application appId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(appId, constructPath(appId));
  }

  /**
   * Removes all tags from an artifact.
   *
   * @param artifactId artifact to remove tags from
   */
  public void removeTags(Id.Artifact artifactId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(artifactId, constructPath(artifactId));
  }

  /**
   * Removes all tags from a dataset.
   *
   * @param datasetInstance dataset to remove tags from
   */
  public void removeTags(Id.DatasetInstance datasetInstance)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(datasetInstance, constructPath(datasetInstance));
  }

  /**
   * Removes all tags from a stream.
   *
   * @param streamId stream to remove tags from
   */
  public void removeTags(Id.Stream streamId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(streamId, constructPath(streamId));
  }

  /**
   * Removes all tags from a view.
   *
   * @param viewId view to remove tags from
   */
  public void removeTags(Id.Stream.View viewId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(viewId, constructPath(viewId));
  }

  /**
   * Removes all tags from a program.
   *
   * @param programId program to remove tags from
   */
  public void removeTags(Id.Program programId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeTags(programId, constructPath(programId));
  }

  private void removeTags(Id.NamespacedId namespacedId, String entityPath)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/tags", entityPath);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

  /**
   * Removes metadata from an application.
   *
   * @param appId app to remove metadata from
   */
  public void removeMetadata(Id.Application appId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(appId, constructPath(appId));
  }

  /**
   * Removes metadata from an artifact.
   *
   * @param artifactId artifact to remove metadata from
   */
  public void removeMetadata(Id.Artifact artifactId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(artifactId, constructPath(artifactId));
  }

  /**
   * Removes metadata from a dataset.
   *
   * @param datasetInstance dataset to remove metadata from
   */
  public void removeMetadata(Id.DatasetInstance datasetInstance)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(datasetInstance, constructPath(datasetInstance));
  }

  /**
   * Removes metadata from a stream.
   *
   * @param streamId stream to remove metadata from
   */
  public void removeMetadata(Id.Stream streamId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(streamId, constructPath(streamId));
  }

  /**
   * Removes metadata from a view.
   *
   * @param viewId view to remove metadata from
   */
  public void removeMetadata(Id.Stream.View viewId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(viewId, constructPath(viewId));
  }

  /**
   * Removes metadata from a program.
   *
   * @param programId program to remove metadata from
   */
  public void removeMetadata(Id.Program programId)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    removeMetadata(programId, constructPath(programId));
  }

  private void removeMetadata(Id.NamespacedId namespacedId, String entityPath)
    throws UnauthenticatedException, BadRequestException, NotFoundException, IOException, UnauthorizedException {
    String path = String.format("%s/metadata", entityPath);
    makeRequest(namespacedId, path, HttpMethod.DELETE);
  }

  private HttpResponse makeRequest(Id.NamespacedId namespacedId, String path, HttpMethod httpMethod)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    return makeRequest(namespacedId, path, httpMethod, null);
  }

  // makes a request and throws BadRequestException or NotFoundException, as appropriate
  private HttpResponse makeRequest(Id.NamespacedId namespacedId, String path,
                                   HttpMethod httpMethod, @Nullable String body)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException, UnauthorizedException {
    URL url = resolve(namespacedId.getNamespace(), path);
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
      throw new NotFoundException(namespacedId.toEntityId());
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

  private String constructPath(Id.Stream.View viewId) {
    return String.format("streams/%s/views/%s", viewId.getStreamId(), viewId.getId());
  }
}

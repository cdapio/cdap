/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResponseV2;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.handler.codec.http.HttpResponseStatus;

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
  private static final Type SET_METADATA_RECORD_V2_TYPE = new TypeToken<Set<MetadataRecordV2>>() { }.getType();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type SET_STRING_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();

  public static final BiMap<String, String> ENTITY_TYPE_TO_API_PART;

  // Metadata endpoints uses names like 'apps' to represent application 'namespaces' to represent namespace so this map
  // is needed to convert one into another so that we can create MetdataEntity/EntityId appropriately.
  static {
    BiMap<String, String> map = HashBiMap.create();
    map.put(MetadataEntity.NAMESPACE, "namespaces");
    map.put(MetadataEntity.APPLICATION, "apps");
    map.put(MetadataEntity.DATASET, "datasets");
    map.put(MetadataEntity.VERSION, "versions");
    map.put(MetadataEntity.ARTIFACT, "artifacts");
    map.put(MetadataEntity.PROGRAM, "programs");
    map.put(MetadataEntity.PROGRAM_RUN, "runs");
    ENTITY_TYPE_TO_API_PART = map;
  }

  /**
   * Executes an HTTP request.
   */
  protected abstract HttpResponse execute(HttpRequest request,  int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, UnauthorizedException;

  /**
   * Resolves the specified URL with the specified namespace
   */
  protected abstract URL resolve(NamespaceId namesapace, String resource) throws IOException;

  /**
   * Resolved the specified URL
   */
  protected abstract URL resolve(String resource) throws IOException;

  /**
   * Searches entities in the specified namespace whose metadata matches the specified query.
   *
   * @param namespace the namespace to search in
   * @param query the query string with which to search
   * @param target the target type. If null, all possible types will be searched
   * @return the {@link MetadataSearchResponse} for the given query.
   */
  public MetadataSearchResponse searchMetadata(NamespaceId namespace, String query,
                                               @Nullable EntityTypeSimpleName target)
    throws IOException, UnauthenticatedException, UnauthorizedException, BadRequestException {
    Set<EntityTypeSimpleName> targets = ImmutableSet.of();
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
   * @param targets {@link EntityTypeSimpleName}s to search. If empty, all possible types will be searched
   * @return A set of {@link MetadataSearchResultRecord} for the given query.
   */
  public MetadataSearchResponse searchMetadata(NamespaceId namespace, String query,
                                               Set<EntityTypeSimpleName> targets)
    throws IOException, UnauthenticatedException, UnauthorizedException, BadRequestException {
    return searchMetadata(namespace, query, targets, null, 0, Integer.MAX_VALUE, 0, null, false);
  }

  /**
   * Searches entities in the specified namespace whose metadata matches the specified query.
   *
   * @param namespace the namespace to search in
   * @param query the query string with which to search
   * @param targets {@link EntityTypeSimpleName}s to search. If empty, all possible types will be searched
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
   * @deprecated since 5.0 replaces by {@link #searchMetadata(NamespaceId, String, Set, String, int, int, int, String,
   * boolean, boolean)}
   */
  public MetadataSearchResponse searchMetadata(NamespaceId namespace, String query,
                                               Set<EntityTypeSimpleName> targets, @Nullable String sort,
                                               int offset, int limit, int numCursors,
                                               @Nullable String cursor, boolean showHidden)
    throws IOException, UnauthenticatedException, UnauthorizedException, BadRequestException {

    HttpResponse response = searchMetadataHelper(namespace, query, targets, sort, offset, limit, numCursors, cursor,
                                                 showHidden, false);
    return GSON.fromJson(response.getResponseBodyAsString(), MetadataSearchResponse.class);
  }

  /**
   * Searches entities in the specified namespace whose metadata matches the specified query.
   *
   * @param namespace the namespace to search in or null if it is a cross namespace search
   * @param query the query string with which to search
   * @param targets {@link EntityTypeSimpleName}s to search. If empty, all possible types will be searched
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
   * @param showCustom boolean which specifies whether to display custom entities or not.
   * @return A set of {@link MetadataSearchResponseV2} for the given query.
   */
  public MetadataSearchResponseV2 searchMetadata(@Nullable NamespaceId namespace, String query,
                                                 Set<EntityTypeSimpleName> targets, @Nullable String sort,
                                                 int offset, int limit, int numCursors,
                                                 @Nullable String cursor, boolean showHidden, boolean showCustom)
    throws IOException, UnauthenticatedException, UnauthorizedException, BadRequestException {

    HttpResponse response = searchMetadataHelper(namespace, query, targets, sort, offset, limit, numCursors, cursor,
                                                 showHidden, showCustom);
    return GSON.fromJson(response.getResponseBodyAsString(), MetadataSearchResponseV2.class);
  }

  private HttpResponse searchMetadataHelper(@Nullable NamespaceId namespace, String query,
                                            Set<EntityTypeSimpleName> targets,
                                            @Nullable String sort, int offset, int limit, int numCursors,
                                            @Nullable String cursor, boolean showHidden, boolean showCustom)
    throws IOException, UnauthenticatedException, BadRequestException {
    StringBuilder path = new StringBuilder();
    if (namespace != null) {
      path.append("namespaces/").append(namespace.getNamespace()).append("/");
    }
    path.append("metadata/search?query=").append(query);
    for (EntityTypeSimpleName t : targets) {
      path.append("&target=").append(t);
    }
    if (sort != null) {
      path.append("&sort=").append(URLEncoder.encode(sort, "UTF-8"));
    }
    path.append("&offset=").append(offset);
    path.append("&limit=").append(limit);
    path.append("&numCursors=").append(numCursors);
    if (cursor != null) {
      path.append("&cursor=").append(cursor);
    }
    if (showHidden) {
      path.append("&showHidden=" + true);
    }
    if (showCustom) {
      path.append("&showCustom=" + true);
    }
    URL searchURL = resolve(path.toString());
    HttpResponse response = execute(HttpRequest.get(searchURL).build(), HttpResponseStatus.BAD_REQUEST.code());
    if (HttpResponseStatus.BAD_REQUEST.code() == response.getResponseCode()) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
    return response;
  }

  /**
   * @param metadataEntity the {@link MetadataEntity} for which to retrieve metadata across
   * {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata for the entity.
   */
  public Set<MetadataRecordV2> getMetadata(MetadataEntity metadataEntity)
    throws UnauthenticatedException, BadRequestException, IOException, UnauthorizedException {
    return getMetadata(metadataEntity, null);
  }

  /**
   * @param metadataEntity the {@link MetadataEntity} for which to retrieve metadata
   * @param scope the {@link MetadataScope} to retrieve the metadata from. If null, this method retrieves
   *              metadata from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata for the entity.
   */
  public Set<MetadataRecordV2> getMetadata(MetadataEntity metadataEntity, @Nullable MetadataScope scope)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    // for new getMetadata which takes MetadataEntity we don't want to do any aggregation of metadata for runId
    // CDAP-13721
    HttpResponse response = getMetadataHelper(metadataEntity, scope, true, false);
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_RECORD_V2_TYPE);
  }

  /**
   * @param entityId the {@link EntityId} for which to retrieve metadata
   * @param scope the {@link MetadataScope} to retrieve the metadata from. If null, this method retrieves
   *              metadata from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata for the entity.
   */
  public Set<MetadataRecord> getMetadata(EntityId entityId, @Nullable MetadataScope scope)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    // for old getMetadata which takes EntityId we want to do any aggregation of metadata for runId for backward
    // compatibility CDAP-13721
    HttpResponse response = getMetadataHelper(entityId.toMetadataEntity(), scope, false, true);
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_RECORD_TYPE);
  }

  /**
   * @param id the entity for which to retrieve metadata across {@link MetadataScope#SYSTEM} and
   * {@link MetadataScope#USER}
   * @return The metadata for the entity.
   */
  public Set<MetadataRecord> getMetadata(EntityId id)
    throws UnauthenticatedException, BadRequestException, IOException, UnauthorizedException {
    return getMetadata(id, null);
  }

  private HttpResponse getMetadataHelper(MetadataEntity metadataEntity, @Nullable MetadataScope scope,
                                         boolean includeCustom, boolean aggregateRun)
    throws IOException, UnauthenticatedException, BadRequestException {
    String path = String.format("%s/metadata", constructPath(metadataEntity));
    path = addQueryParams(path, metadataEntity, scope, includeCustom, aggregateRun);
    return makeRequest(path, HttpMethod.GET, null);
  }

  /**
   * @param id the entity for which to retrieve metadata properties across {@link MetadataScope#SYSTEM} and
   * {@link MetadataScope#USER}
   * @return The metadata properties for the entity.
   */
  public Map<String, String> getProperties(EntityId id)
    throws UnauthenticatedException, BadRequestException, IOException, UnauthorizedException {
    return getProperties(id, null);
  }

  /**
   * @param id the entity for which to retrieve metadata properties
   * @param scope the {@link MetadataScope} to retrieve the properties from. If null, this method retrieves
   *              properties from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata properties for the entity.
   */
  public Map<String, String> getProperties(EntityId id, @Nullable MetadataScope scope)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    return getProperties(id.toMetadataEntity(), scope);
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
    return getTags(id.toMetadataEntity(), scope);
  }

  /**
   * @param metadataEntity the {@link MetadataEntity} for which to retrieve metadata tags
   * across {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the entity.
   */
  public Set<String> getTags(MetadataEntity metadataEntity) throws UnauthenticatedException, BadRequestException,
    IOException, UnauthorizedException, NotFoundException {
    return getTags(metadataEntity, null);
  }

  /**
   * @param metadataEntity the {@link MetadataEntity} for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If null, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the entity.
   */
  public Set<String> getTags(MetadataEntity metadataEntity, @Nullable MetadataScope scope)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException, NotFoundException {
    String path = String.format("%s/metadata/tags", constructPath(metadataEntity));
    path = addQueryParams(path, metadataEntity, scope, true);
    HttpResponse response = makeRequest(path, HttpMethod.GET, null);
    return GSON.fromJson(response.getResponseBodyAsString(), SET_STRING_TYPE);
  }

  /**
   * @param id the entity for which to add metadata properties
   * @param properties the metadata properties
   */
  public void addProperties(EntityId id, Map<String, String> properties)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    addProperties(id.toMetadataEntity(), properties);
  }

  /**
   * @param id the entity for which to add metadata tags
   * @param tags the metadata tags
   */
  public void addTags(EntityId id, Set<String> tags)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    addTags(id.toMetadataEntity(), tags);
  }

  /**
   * @param metadataEntity the {@link MetadataEntity} for which to add metadata tags
   * @param tags the metadata tags
   */
  public void addTags(MetadataEntity metadataEntity, Set<String> tags) throws BadRequestException,
    UnauthenticatedException, IOException, UnauthorizedException {
    String path = String.format("%s/metadata/tags", constructPath(metadataEntity));
    path = addQueryParams(path, metadataEntity, null, false);
    makeRequest(path, HttpMethod.POST, GSON.toJson(tags));
  }

  /**
   * @param id the entity for which to remove metadata
   */
  public void removeMetadata(EntityId id)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    removeMetadata(id.toMetadataEntity());
  }

  /**
   * @param id the entity for which to remove metadata properties
   */
  public void removeProperties(EntityId id)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    removeProperties(id.toMetadataEntity());
  }

  /**
   * @param id the entity for which to remove metadata tags
   */
  public void removeTags(EntityId id)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    removeTags(id.toMetadataEntity());
  }

  /**
   * @param id the entity for which to remove a metadata property
   * @param property the property to remove
   */
  public void removeProperty(EntityId id, String property)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    removeProperty(id.toMetadataEntity(), property);
  }

  /**
   * @param id the entity for which to remove a metadata tag
   * @param tag the tag to remove
   */
  public void removeTag(EntityId id, String tag)
    throws BadRequestException, UnauthenticatedException, IOException, UnauthorizedException {
    removeTag(id.toMetadataEntity(), tag);
  }

  /**
   * @param metadataEntity the {@link MetadataEntity} for which to retrieve the metadata properties
   * @return The metadata properties for the entity.
   */
  public Map<String, String> getProperties(MetadataEntity metadataEntity)
    throws UnauthenticatedException, BadRequestException, IOException, UnauthorizedException {
    return getProperties(metadataEntity, null);
  }

  /**
   * Return the properties for the {@link MetadataEntity} in the given scope
   * @param metadataEntity whose properties is needed
   * @param scope the scope of properties
   * @return a map of properties
   */
  public Map<String, String> getProperties(MetadataEntity metadataEntity, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/properties", constructPath(metadataEntity));
    path = addQueryParams(path, metadataEntity, scope, false);
    HttpResponse response = makeRequest(path, HttpMethod.GET, null);
    return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
  }

  /**
   * Adds properties to an application.
   *
   * @param metadataEntity app to add properties to
   * @param properties properties to be added
   */
  public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties)
    throws IOException, UnauthenticatedException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/properties", constructPath(metadataEntity));
    path = addQueryParams(path, metadataEntity, null, false);
    makeRequest(path, HttpMethod.POST, GSON.toJson(properties));
  }

  /**
   * Removes the given property from the given {@link MetadataEntity}.
   *
   * @param metadataEntity to remove properties from
   * @param propertyToRemove the property to remove
   */
  public void removeProperty(MetadataEntity metadataEntity, String propertyToRemove)
    throws IOException, UnauthenticatedException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/properties/%s", constructPath(metadataEntity), propertyToRemove);
    path = addQueryParams(path, metadataEntity, null, false);
    makeRequest(path, HttpMethod.DELETE, null);
  }

  /**
   * Removes all properties from the given {@link MetadataEntity}.
   *
   * @param metadataEntity to remove properties from
   */
  public void removeProperties(MetadataEntity metadataEntity)
    throws IOException, UnauthenticatedException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/properties", constructPath(metadataEntity));
    path = addQueryParams(path, metadataEntity, null, false);
    makeRequest(path, HttpMethod.DELETE, null);
  }

  /**
   * Removes the given tag from the given {@link MetadataEntity}
   * @param metadataEntity the {@link MetadataEntity} from which the given tag needs to be removed
   * @param tagToRemove the tag to removed
   */
  public void removeTag(MetadataEntity metadataEntity, String tagToRemove)
    throws IOException, UnauthenticatedException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/tags/%s", constructPath(metadataEntity), tagToRemove);
    path = addQueryParams(path, metadataEntity, null, false);
    makeRequest(path, HttpMethod.DELETE, null);
  }

  /**
   * Removes tags from the given {@link MetadataEntity}
   *
   * @param metadataEntity the {@link MetadataEntity} from which tags needs to be removed
   */
  public void removeTags(MetadataEntity metadataEntity)
    throws IOException, UnauthenticatedException, BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata/tags", constructPath(metadataEntity));
    path = addQueryParams(path, metadataEntity, null, false);
    makeRequest(path, HttpMethod.DELETE, null);
  }

  /**
   * Removes metadata from the given {@link MetadataEntity}
   *
   * @param metadataEntity the {@link MetadataEntity} from which metadata needs to be removed
   */
  public void removeMetadata(MetadataEntity metadataEntity) throws IOException, UnauthenticatedException,
    BadRequestException, UnauthorizedException {
    String path = String.format("%s/metadata", constructPath(metadataEntity));
    path = addQueryParams(path, metadataEntity, null, false);
    makeRequest(path, HttpMethod.DELETE, null);
  }

  private HttpResponse makeRequest(String path, HttpMethod httpMethod, @Nullable String body)
    throws IOException, UnauthenticatedException, BadRequestException, UnauthorizedException {
    URL url = resolve(path);
    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url);
    if (body != null) {
      builder.withBody(body);
    }
    HttpResponse response = execute(builder.build(),
                                    HttpURLConnection.HTTP_BAD_REQUEST, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
    return response;
  }

  // construct a component of the path, specific to each entity type
  private String constructPath(MetadataEntity metadataEntity) {
    StringBuilder builder = new StringBuilder();
    metadataEntity.iterator().forEachRemaining(keyValue -> {
      if (ENTITY_TYPE_TO_API_PART.containsKey(keyValue.getKey())) {
        builder.append(ENTITY_TYPE_TO_API_PART.get(keyValue.getKey()));
      } else {
        builder.append(keyValue.getKey());
      }
      builder.append("/");
      builder.append(keyValue.getValue());
      builder.append("/");
    });
    // remove the last /
    builder.replace(builder.length() - 1, builder.length(), "");
    return builder.toString();
  }

  private String addQueryParams(String path, MetadataEntity metadataEntity, @Nullable  MetadataScope scope,
                                boolean showCustom) {
    // for all other queries except getMetadata called on EntityId we don't want aggregation of metadata for runIds.
    // CDAP-13721
    return addQueryParams(path, metadataEntity, scope, showCustom, false);
  }

  private String addQueryParams(String path, MetadataEntity metadataEntity, @Nullable  MetadataScope scope,
                                boolean showCustom, boolean aggregateRun) {
    StringBuilder builder = new StringBuilder(path);
    String prefix = "?";
    if (!Iterables.getLast(metadataEntity.getKeys()).equalsIgnoreCase(metadataEntity.getType())) {
      // if last leaf node is not the entity type specify it through query para
      builder.append(prefix);
      builder.append("type=");
      builder.append(metadataEntity.getType());
      prefix = "&";
    }
    if (showCustom) {
      builder.append(prefix);
      builder.append("showCustom=");
      builder.append(true);
      prefix = "&";
    }
    // the default value of aggregateRun is true so in the case it is false we need to include it as query param
    if (!aggregateRun) {
      builder.append(prefix);
      builder.append("aggregateRun=");
      builder.append(false);
      prefix = "&";
    }
    if (scope == null) {
      return builder.toString();
    } else {
      builder.append(prefix);
      builder.append("scope=");
      builder.append(scope);
    }
    return builder.toString();
  }
}

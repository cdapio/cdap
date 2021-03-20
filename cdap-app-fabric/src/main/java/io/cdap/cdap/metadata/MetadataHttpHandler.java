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

package io.cdap.cdap.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.client.MetadataClient;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.data2.metadata.MetadataCompatibility;
import io.cdap.cdap.metadata.elastic.ScopedNameOfKindTypeAdapter;
import io.cdap.cdap.metadata.elastic.ScopedNameTypeAdapter;
import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.codec.NamespacedEntityIdCodec;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataCodec;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataMutationCodec;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import io.cdap.cdap.spi.metadata.Sorting;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * HttpHandler for Metadata
 */
@Path(Constants.Gateway.API_VERSION_3)
public class MetadataHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataHttpHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .registerTypeAdapter(Metadata.class, new MetadataCodec())
    .create();
  // for internal calls (create/update/drop/delete) we need to use a different codec for ScopedName and
  // ScopedNameOfKind: they are used as map keys, where JSON only allows plain strings, so we serialize
  // as [kind:]scope:name instead of a record. In other methods (the external metadata endpoint), however,
  // we need to keep the serialization as a json record.
  private static final Gson GSON_INTERNAL = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .registerTypeAdapter(Metadata.class, new MetadataCodec())
    .registerTypeAdapter(ScopedName.class, new ScopedNameTypeAdapter())
    .registerTypeAdapter(ScopedNameOfKind.class, new ScopedNameOfKindTypeAdapter())
    .registerTypeAdapter(MetadataMutation.class, new MetadataMutationCodec())
    .create();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type SET_STRING_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Type LIST_MUTATION_TYPE = new TypeToken<List<MetadataMutation>>() { }.getType();
  private static final MutationOptions SYNC = MutationOptions.builder().setAsynchronous(false).build();
  private static final MutationOptions ASYNC = MutationOptions.builder().setAsynchronous(true).build();

  private final MetadataAdmin metadataAdmin;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  MetadataHttpHandler(MetadataAdmin metadataAdmin,
                      AuthorizationEnforcer authorizationEnforcer, AuthenticationContext authenticationContext) {
    this.metadataAdmin = metadataAdmin;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @GET
  @Path("/**/metadata")
  public void getMetadata(HttpRequest request, HttpResponder responder,
                          @Nullable @QueryParam("scope") String scope,
                          @Nullable @QueryParam("type") String type,
                          @Nullable @QueryParam("responseFormat") @DefaultValue("v5") String responseFormat)
    throws Exception {
    MetadataEntity entity = getMetadataEntityFromPath(request.uri(), type, "/metadata");
    if (isEntityType(entity)) {
      AuthorizationUtil.ensureAccess(EntityId.fromMetadataEntity(entity),
                                     authorizationEnforcer, authenticationContext.getPrincipal());
    }
    MetadataScope theScope = validateScope(scope);
    Metadata metadata = scope == null ? metadataAdmin.getMetadata(entity) : metadataAdmin.getMetadata(entity, theScope);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(
      "v5".equals(responseFormat) ? MetadataCompatibility.toV5MetadataRecords(entity, metadata, scope) : metadata));
  }

  @GET
  @Path("/**/metadata/properties")
  public void getProperties(HttpRequest request, HttpResponder responder,
                            @QueryParam("scope") String scope,
                            @QueryParam("type") String type,
                            @Nullable @QueryParam("responseFormat") @DefaultValue("v5") String responseFormat)
    throws Exception {
    MetadataEntity entity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    if (isEntityType(entity)) {
      AuthorizationUtil.ensureAccess(EntityId.fromMetadataEntity(entity),
                                     authorizationEnforcer, authenticationContext.getPrincipal());
    }
    MetadataScope theScope = validateScope(scope);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(
      "v5".equals(responseFormat)
        ? (scope == null ? metadataAdmin.getProperties(entity) : metadataAdmin.getProperties(theScope, entity))
        : metadataAdmin.getMetadata(entity, theScope, MetadataKind.PROPERTY)));
  }

  @GET
  @Path("/**/metadata/tags")
  public void getTags(HttpRequest request, HttpResponder responder,
                      @QueryParam("scope") String scope,
                      @QueryParam("type") String type,
                      @Nullable @QueryParam("responseFormat") @DefaultValue("v5") String responseFormat)
    throws Exception {
    MetadataEntity entity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    if (isEntityType(entity)) {
      AuthorizationUtil.ensureAccess(EntityId.fromMetadataEntity(entity),
                                     authorizationEnforcer, authenticationContext.getPrincipal());
    }
    MetadataScope theScope = validateScope(scope);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(
      "v5".equals(responseFormat)
        ? (scope == null ? metadataAdmin.getTags(entity) : metadataAdmin.getTags(theScope, entity))
        : metadataAdmin.getMetadata(entity, theScope, MetadataKind.TAG)));
  }

  @POST
  @Path("/**/metadata/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addProperties(FullHttpRequest request, HttpResponder responder,
                            @QueryParam("type") String type,
                            @QueryParam("async") @DefaultValue("false") Boolean async)
    throws Exception {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    enforce(metadataEntity, Action.ADMIN);
    metadataAdmin.addProperties(metadataEntity, readProperties(request), async ? ASYNC : SYNC);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for %s added successfully.", metadataEntity));
  }

  @POST
  @Path("/**/metadata/tags")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addTags(FullHttpRequest request, HttpResponder responder,
                      @QueryParam("type") String type,
                      @QueryParam("async") @DefaultValue("false") Boolean async)
    throws Exception {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    enforce(metadataEntity, Action.ADMIN);
    metadataAdmin.addTags(metadataEntity, readTags(request), async ? ASYNC : SYNC);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tags for %s added successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata")
  public void removeMetadata(HttpRequest request, HttpResponder responder,
                             @QueryParam("type") String type,
                             @QueryParam("async") @DefaultValue("false") Boolean async) throws Exception {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata");
    enforce(metadataEntity, Action.ADMIN);
    metadataAdmin.removeMetadata(metadataEntity, async ? ASYNC : SYNC);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/properties")
  public void removeProperties(HttpRequest request, HttpResponder responder,
                               @QueryParam("type") String type,
                               @QueryParam("async") @DefaultValue("false") Boolean async) throws Exception {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    enforce(metadataEntity, Action.ADMIN);
    metadataAdmin.removeProperties(metadataEntity, async ? ASYNC : SYNC);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/properties/{property}")
  public void removeProperty(HttpRequest request, HttpResponder responder,
                             @PathParam("property") String property,
                             @QueryParam("type") String type,
                             @QueryParam("async") @DefaultValue("false") Boolean async) throws Exception {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    enforce(metadataEntity, Action.ADMIN);
    metadataAdmin.removeProperties(metadataEntity, Collections.singleton(property), async ? ASYNC : SYNC);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for %s deleted successfully.", property, metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/tags")
  public void removeTags(HttpRequest request, HttpResponder responder,
                         @QueryParam("type") String type,
                         @QueryParam("async") @DefaultValue("false") Boolean async) throws Exception {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    enforce(metadataEntity, Action.ADMIN);
    metadataAdmin.removeTags(metadataEntity, async ? ASYNC : SYNC);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tags for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/tags/{tag}")
  public void removeTag(HttpRequest request, HttpResponder responder,
                        @PathParam("tag") String tag,
                        @QueryParam("type") String type,
                        @QueryParam("async") @DefaultValue("false") Boolean async) throws Exception {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    enforce(metadataEntity, Action.ADMIN);
    metadataAdmin.removeTags(metadataEntity, Collections.singleton(tag), async ? ASYNC : SYNC);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tag %s for %s deleted successfully.", tag, metadataEntity));
  }

  @POST
  @Path("/metadata-internals/create")
  public void create(FullHttpRequest request, HttpResponder responder) throws IOException {
    MetadataMutation.Create createMutation =
      GSON_INTERNAL.fromJson(request.content().toString(StandardCharsets.UTF_8), MetadataMutation.Create.class);
    metadataAdmin.applyMutation(createMutation, SYNC);
    responder.sendString(HttpResponseStatus.OK, "Create Metadata mutation applied successfully.");
  }

  @POST
  @Path("/metadata-internals/update")
  public void update(FullHttpRequest request, HttpResponder responder) throws IOException {
    MetadataMutation.Update updateMutation =
      GSON_INTERNAL.fromJson(request.content().toString(StandardCharsets.UTF_8), MetadataMutation.Update.class);
    metadataAdmin.applyMutation(updateMutation, SYNC);
    responder.sendString(HttpResponseStatus.OK, "Update Metadata mutation applied successfully.");
  }

  @DELETE
  @Path("/metadata-internals/drop")
  public void drop(FullHttpRequest request, HttpResponder responder) throws IOException {
    MetadataMutation.Drop dropMutation =
      GSON_INTERNAL.fromJson(request.content().toString(StandardCharsets.UTF_8), MetadataMutation.Drop.class);
    metadataAdmin.applyMutation(dropMutation, SYNC);
    responder.sendString(HttpResponseStatus.OK, "Drop Metadata mutation applied successfully.");
  }

  @DELETE
  @Path("/metadata-internals/remove")
  public void remove(FullHttpRequest request, HttpResponder responder) throws IOException {
    MetadataMutation.Remove removeMutation =
      GSON_INTERNAL.fromJson(request.content().toString(StandardCharsets.UTF_8), MetadataMutation.Remove.class);
    metadataAdmin.applyMutation(removeMutation, SYNC);
    responder.sendString(HttpResponseStatus.OK, "Remove Metadata mutation applied successfully.");
  }

  @POST
  @Path("/metadata-internals/batch")
  public void batch(FullHttpRequest request, HttpResponder responder) throws IOException {
    List<MetadataMutation> mutations =
      GSON_INTERNAL.fromJson(request.content().toString(StandardCharsets.UTF_8), LIST_MUTATION_TYPE);
    metadataAdmin.applyMutations(mutations, SYNC);
    responder.sendString(HttpResponseStatus.OK, "List of Metadata mutations applied successfully.");
  }

  @GET
  @Path("/metadata/search")
  public void searchMetadata(HttpRequest request, HttpResponder responder,
                             @Nullable @QueryParam("namespaces") List<String> namespaces,
                             @Nullable @QueryParam("scope") String scope,
                             @Nullable @QueryParam("query") String searchQuery,
                             @Nullable @QueryParam("target") List<String> targets,
                             @Nullable @QueryParam("sort") String sort,
                             @QueryParam("offset") @DefaultValue("0") int offset,
                             // 2147483647 is Integer.MAX_VALUE
                             @QueryParam("limit") @DefaultValue("2147483647") int limit,
                             @Nullable @QueryParam("numCursors") Integer numCursors,
                             @QueryParam("cursorRequested") @DefaultValue("false") boolean cursorRequested,
                             @Nullable @QueryParam("cursor") String cursor,
                             @QueryParam("showHidden") @DefaultValue("false") boolean showHidden,
                             @Nullable @QueryParam("entityScope") String entityScope,
                             @Nullable @QueryParam("responseFormat") @DefaultValue("v5") String responseFormat)
    throws Exception {
    SearchRequest searchRequest = getValidatedSearchRequest(scope, namespaces, searchQuery, targets, sort,
                                                            offset, limit, numCursors, cursorRequested, cursor,
                                                            showHidden, entityScope);
    SearchResponse response = metadataAdmin.search(searchRequest);
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson("v5".equals(responseFormat)
                                     ? MetadataCompatibility.toV5Response(response, entityScope) : response));
  }

  @GET
  @Path("/namespaces/{namespace-id}/metadata/search")
  public void searchNamespace(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @Nullable @QueryParam("scope") String scope,
                              @Nullable @QueryParam("query") String searchQuery,
                              @Nullable @QueryParam("target") List<String> targets,
                              @Nullable @QueryParam("sort") String sort,
                              @QueryParam("offset") @DefaultValue("0") int offset,
                              // 2147483647 is Integer.MAX_VALUE
                              @QueryParam("limit") @DefaultValue("2147483647") int limit,
                              @Nullable @QueryParam("numCursors") Integer numCursors,
                              @QueryParam("cursorRequested") @DefaultValue("false") boolean cursorRequested,
                              @Nullable @QueryParam("cursor") String cursor,
                              @QueryParam("showHidden") @DefaultValue("false") boolean showHidden,
                              @Nullable @QueryParam("entityScope") String entityScope,
                              @Nullable @QueryParam("responseFormat") @DefaultValue("v5") String responseFormat)
    throws Exception {
    SearchRequest searchRequest = getValidatedSearchRequest(scope, ImmutableList.of(namespaceId), searchQuery, targets,
                                                            sort, offset, limit, numCursors, cursorRequested, cursor,
                                                            showHidden, entityScope);
    SearchResponse response = metadataAdmin.search(searchRequest);
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson("v5".equals(responseFormat)
                                     ? MetadataCompatibility.toV5Response(response, entityScope) : response));
  }

  // TODO (CDAP-14946): Find a better way to determine allowed combinations of search parameters
  private SearchRequest getValidatedSearchRequest(@Nullable String scope,
                                                  @Nullable List<String> namespaces,
                                                  @Nullable String searchQuery,
                                                  @Nullable List<String> targets,
                                                  @Nullable String sort,
                                                  int offset, int limit,
                                                  @Nullable Integer numCursors,
                                                  boolean cursorRequested,
                                                  @Nullable String cursor,
                                                  boolean showHidden,
                                                  @Nullable String entityScope) throws BadRequestException {
    try {
      SearchRequest.Builder builder = SearchRequest.of(searchQuery == null ? "*" : searchQuery);
      if (scope != null) {
        builder.setScope(validateScope(scope));
      }
      if (EntityScope.SYSTEM == validateEntityScope(entityScope)) {
        builder.addNamespace(entityScope.toLowerCase());
      } else if (namespaces != null) {
        for (String namespace : namespaces) {
          builder.addNamespace(namespace);
        }
      }
      if (targets != null) {
        targets.forEach(builder::addType);
      }
      if (sort != null) {
        Sorting sorting;
        try {
          sorting = Sorting.of(URLDecoder.decode(sort, StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException e) {
          // this cannot happen because UTF_8 is always supported
          throw new IllegalStateException(e);
        }
        if (!MetadataConstants.ENTITY_NAME_KEY.equalsIgnoreCase(sorting.getKey()) &&
          !MetadataConstants.CREATION_TIME_KEY.equalsIgnoreCase(sorting.getKey())) {
          throw new IllegalArgumentException("Sorting is only supported on fields: " +
                                               MetadataConstants.ENTITY_NAME_KEY + ", " +
                                               MetadataConstants.CREATION_TIME_KEY);
        }
        builder.setSorting(sorting);
      }
      builder.setOffset(offset);
      builder.setLimit(limit);
      if (cursorRequested || (numCursors != null && numCursors > 0)) {
        if (sort == null) {
          throw new IllegalArgumentException("Specify a sort order when requesting a cursor");
        }
        builder.setCursorRequested(true);
      }
      if (cursor != null) {
        if (sort == null) {
          throw new IllegalArgumentException("Specify a sort order when passing in a cursor");
        }
        builder.setCursor(cursor);
      }
      builder.setShowHidden(showHidden);
      SearchRequest request = builder.build();
      LOG.trace("Received search request {}", request);
      return request;
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
  }

  private MetadataEntity getMetadataEntityFromPath(String uri, @Nullable String entityType, String suffix) {
    String[] parts = uri.substring((uri.indexOf(Constants.Gateway.API_VERSION_3) +
      Constants.Gateway.API_VERSION_3.length() + 1), uri.lastIndexOf(suffix)).split("/");
    MetadataEntity.Builder builder = MetadataEntity.builder();

    int curIndex = 0;
    while (curIndex < parts.length - 1) {
      // the api part which we get for program does not have 'type' keyword as part of the uri. It goes like
      // ../apps/appName/programType/ProgramName so handle that correctly
      if (curIndex >= 2 && parts[curIndex - 2].equalsIgnoreCase("apps") && !parts[curIndex].equalsIgnoreCase
        ("versions")) {
        builder = appendHelper(builder, entityType, MetadataEntity.TYPE,
                                      ProgramType.valueOfCategoryName(parts[curIndex]).name());
        builder = appendHelper(builder, entityType, MetadataEntity.PROGRAM, parts[curIndex + 1]);
      } else {
        if (MetadataClient.ENTITY_TYPE_TO_API_PART.inverse().containsKey(parts[curIndex])) {
          builder = appendHelper(builder, entityType,
                                        MetadataClient.ENTITY_TYPE_TO_API_PART.inverse().get(parts[curIndex]),
                                        parts[curIndex + 1]);
        } else {
          builder = appendHelper(builder, entityType, parts[curIndex], parts[curIndex + 1]);
        }
      }
      curIndex += 2;
    }
    builder = makeBackwardCompatible(builder);
    return builder.build();
  }

  /**
   * If the MetadataEntity Builder key-value represent an application or artifact which is versioned in CDAP i.e. their
   * metadata entity representation ends with 'version' rather than 'application' or 'artifact' and the type is also
   * set to 'version' then for backward compatibility of rest end points return an updated builder which has the
   * correct type. This is only needed in 5.0 for backward compatibility of the rest endpoint.
   * From 5.0 and later the rest end point must be called with a query parameter which specify the type of the
   * metadata entity if the type is not the last key. (CDAP-13678)
   */
  @VisibleForTesting
  static MetadataEntity.Builder makeBackwardCompatible(MetadataEntity.Builder builder) {
    MetadataEntity entity = builder.build();
    List<MetadataEntity.KeyValue> entityKeyValues = StreamSupport.stream(entity.spliterator(), false)
      .collect(Collectors.toList());
    if (entityKeyValues.size() == 3 && entity.getType().equals(MetadataEntity.VERSION) &&
      (entityKeyValues.get(1).getKey().equals(MetadataEntity.ARTIFACT) ||
        entityKeyValues.get(1).getKey().equals(MetadataEntity.APPLICATION))) {
      // this is artifact or application so update the builder
      MetadataEntity.Builder actualEntityBuilder = MetadataEntity.builder();
      // namespace
      actualEntityBuilder.append(entityKeyValues.get(0).getKey(), entityKeyValues.get(0).getValue());
      // application or artifact (so append as type)
      actualEntityBuilder.appendAsType(entityKeyValues.get(1).getKey(), entityKeyValues.get(1).getValue());
      // version detail
      actualEntityBuilder.append(entityKeyValues.get(2).getKey(), entityKeyValues.get(2).getValue());
      return actualEntityBuilder;
    }
    return builder;
  }

  private MetadataEntity.Builder appendHelper(MetadataEntity.Builder builder, @Nullable String entityType,
                                      String key, String value) {

    if (entityType == null || entityType.isEmpty()) {
      // if a type is not provided then keep appending as type to update the type on every append
      return builder.appendAsType(key, value);
    } else {
      if (entityType.equalsIgnoreCase(key)) {
        // if a type was provided and this key is the type then appendAsType
        return builder.appendAsType(key, value);
      } else {
        return builder.append(key, value);
      }
    }
  }

  private Map<String, String> readProperties(FullHttpRequest request) throws BadRequestException {
    ByteBuf content = request.content();
    if (!content.isReadable()) {
      throw new BadRequestException("Unable to read metadata properties from the request.");
    }

    Map<String, String> metadata;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(content), StandardCharsets.UTF_8)) {
      metadata = GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
    } catch (IOException e) {
      throw new BadRequestException("Unable to read metadata properties from the request.", e);
    }

    if (metadata == null) {
      throw new BadRequestException("Null metadata was read from the request");
    }
    return metadata;
  }

  private Set<String> readTags(FullHttpRequest request) throws BadRequestException {
    ByteBuf content = request.content();
    if (!content.isReadable()) {
      throw new BadRequestException("Unable to read a list of tags from the request.");
    }
    Set<String> toReturn;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(content), StandardCharsets.UTF_8)) {
      toReturn = GSON.fromJson(reader, SET_STRING_TYPE);
    } catch (IOException e) {
      throw new BadRequestException("Unable to read a list of tags from the request.", e);
    }

    if (toReturn == null) {
      throw new BadRequestException("Null tags were read from the request.");
    }
    return toReturn;
  }

  private MetadataScope validateScope(@Nullable String scope) throws BadRequestException {
    try {
      return scope == null ? null : MetadataScope.valueOf(scope.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid metadata scope '%s'. Expected '%s' or '%s'",
                                                  scope, MetadataScope.USER, MetadataScope.SYSTEM));
    }
  }

  @Nullable
  private EntityScope validateEntityScope(@Nullable String entityScope) throws BadRequestException {
    if (entityScope == null) {
      return null;
    }
    try {
      return EntityScope.valueOf(entityScope.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid entity scope '%s'. Expected '%s' or '%s' for entities " +
                                                    "from specified scope, or just omit the parameter to get " +
                                                    "entities from both scopes",
                                                  entityScope, EntityScope.USER, EntityScope.SYSTEM));
    }
  }

  private void enforce(MetadataEntity metadataEntity, Action action) throws Exception {
    if (isEntityType(metadataEntity)) {
      authorizationEnforcer.enforce(EntityId.fromMetadataEntity(metadataEntity), authenticationContext.getPrincipal(),
                                    action);
    }
  }

  /**
   * Check if the metadata entity is an entity type. It is possible that metadata entity has a type which is not among
   * the entity ids. In those cases, do not enforce.
   */
  private boolean isEntityType(MetadataEntity metadataEntity) {
    try {
      EntityType.valueOf(metadataEntity.getType().toUpperCase());
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}

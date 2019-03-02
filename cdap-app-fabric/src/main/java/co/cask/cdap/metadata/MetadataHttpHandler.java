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

package co.cask.cdap.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metadata.MetadataPath;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.data2.metadata.MetadataCompatibility;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataConstants;
import co.cask.cdap.spi.metadata.MetadataRecord;
import co.cask.cdap.spi.metadata.SearchRequest;
import co.cask.cdap.spi.metadata.SearchResponse;
import co.cask.cdap.spi.metadata.Sorting;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
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
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type SET_STRING_TYPE = new TypeToken<Set<String>>() { }.getType();

  private final MetadataAdmin metadataAdmin;

  @Inject
  MetadataHttpHandler(MetadataAdmin metadataAdmin) {
    this.metadataAdmin = metadataAdmin;
  }

  @GET
  @Path("/**/metadata")
  public void getMetadata(HttpRequest request, HttpResponder responder,
                          @Nullable @QueryParam("scope") String scope,
                          @Nullable @QueryParam("type") String type,
                          @Nullable @QueryParam("responseFormat") @DefaultValue("v5") String responseFormat)
    throws BadRequestException, IOException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata");
    Metadata metadata = getMetadata(metadataEntity, scope);
    MetadataRecord record = new MetadataRecord(metadataEntity, metadata);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(
      "v5".equals(responseFormat) ? MetadataCompatibility.toV5MetadataRecords(record, scope) : record));
  }

  @GET
  @Path("/**/metadata/properties")
  public void getProperties(HttpRequest request, HttpResponder responder,
                            @QueryParam("scope") String scope,
                            @QueryParam("type") String type)
    throws BadRequestException, IOException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getProperties(metadataEntity, scope)));
  }

  @GET
  @Path("/**/metadata/tags")
  public void getTags(HttpRequest request, HttpResponder responder,
                      @QueryParam("scope") String scope, @QueryParam("type") String type)
    throws BadRequestException, IOException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getTags(metadataEntity, scope)));
  }

  @POST
  @Path("/**/metadata/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addProperties(FullHttpRequest request, HttpResponder responder,
                            @QueryParam("type") String type) throws BadRequestException, IOException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    metadataAdmin.addProperties(metadataEntity, readProperties(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for %s added successfully.", metadataEntity));
  }

  @POST
  @Path("/**/metadata/tags")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void addTags(FullHttpRequest request, HttpResponder responder,
                      @QueryParam("type") String type) throws BadRequestException, IOException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    metadataAdmin.addTags(metadataEntity, readTags(request));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tags for %s added successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata")
  public void removeMetadata(HttpRequest request, HttpResponder responder,
                             @QueryParam("type") String type) throws IOException, BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata");
    metadataAdmin.removeMetadata(metadataEntity);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/properties")
  public void removeProperties(HttpRequest request, HttpResponder responder,
                               @QueryParam("type") String type) throws IOException, BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    metadataAdmin.removeProperties(metadataEntity);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata properties for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/properties/{property}")
  public void removeProperty(HttpRequest request, HttpResponder responder,
                             @PathParam("property") String property,
                             @QueryParam("type") String type) throws IOException, BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/properties");
    metadataAdmin.removeProperties(metadataEntity, Collections.singleton(property));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata property %s for %s deleted successfully.", property, metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/tags")
  public void removeTags(HttpRequest request, HttpResponder responder,
                         @QueryParam("type") String type) throws IOException, BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    metadataAdmin.removeTags(metadataEntity);
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tags for %s deleted successfully.", metadataEntity));
  }

  @DELETE
  @Path("/**/metadata/tags/{tag}")
  public void removeTag(HttpRequest request, HttpResponder responder,
                        @PathParam("tag") String tag,
                        @QueryParam("type") String type) throws IOException, BadRequestException {
    MetadataEntity metadataEntity = getMetadataEntityFromPath(request.uri(), type, "/metadata/tags");
    metadataAdmin.removeTags(metadataEntity, Collections.singleton(tag));
    responder.sendString(HttpResponseStatus.OK,
                         String.format("Metadata tag %s for %s deleted successfully.", tag, metadataEntity));
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
          throw new IllegalArgumentException("Cursors may only be requested in conjunction with sorting");
        }
        builder.setCursorRequested(true);
      }
      if (cursor != null) {
        if (sort == null) {
          throw new IllegalArgumentException("Cursors are only allowed in conjunction with sorting");
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

  private MetadataEntity getMetadataEntityFromPath(String uri, @Nullable String type, String suffix)
    throws BadRequestException {
    try {
      return MetadataPath.getMetadataEntityFromPath(uri, Constants.Gateway.API_VERSION_3 + "/", suffix, type);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
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

  private Metadata getMetadata(MetadataEntity metadataEntity,
                               @Nullable String scope) throws BadRequestException, IOException {
    return scope == null ? metadataAdmin.getMetadata(metadataEntity)
      : metadataAdmin.getMetadata(validateScope(scope), metadataEntity);
  }

  private Map<String, String> getProperties(MetadataEntity metadataEntity,
                                            @Nullable String scope) throws BadRequestException, IOException {
    return (scope == null) ?
      metadataAdmin.getProperties(metadataEntity) :
      metadataAdmin.getProperties(validateScope(scope), metadataEntity);
  }

  private Set<String> getTags(MetadataEntity metadataEntity,
                              @Nullable String scope) throws BadRequestException, IOException {
    return (scope == null) ?
      metadataAdmin.getTags(metadataEntity) :
      metadataAdmin.getTags(validateScope(scope), metadataEntity);
  }

  private MetadataScope validateScope(String scope) throws BadRequestException {
    try {
      return MetadataScope.valueOf(scope.toUpperCase());
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
}

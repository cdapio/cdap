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

package io.cdap.cdap.gateway.handlers;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.api.artifact.InvalidArtifactRangeException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ArtifactRangeNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.AbstractBodyConsumer;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.WriteConflictException;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.capability.CapabilityNotAvailableException;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactPropertiesRequest;
import io.cdap.cdap.proto.artifact.ArtifactRanges;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.artifact.ArtifactSummaryProperties;
import io.cdap.cdap.proto.artifact.PluginInfo;
import io.cdap.cdap.proto.artifact.PluginSummary;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.Ids;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipException;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link io.cdap.http.HttpHandler} for managing artifacts.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class ArtifactHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactHttpHandler.class);
  private static final String VERSION_HEADER = "Artifact-Version";
  private static final String EXTENDS_HEADER = "Artifact-Extends";
  private static final String PLUGINS_HEADER = "Artifact-Plugins";
  private static final Type APPCLASS_SUMMARIES_TYPE = new TypeToken<List<ApplicationClassSummary>>() { }.getType();
  private static final Type APPCLASS_INFOS_TYPE = new TypeToken<List<ApplicationClassInfo>>() { }.getType();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type BATCH_ARTIFACT_PROPERTIES_REQUEST =
    new TypeToken<List<ArtifactPropertiesRequest>>() { }.getType();
  private static final Type BATCH_ARTIFACT_PROPERTIES_RESPONSE =
    new TypeToken<List<ArtifactSummaryProperties>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Type PLUGINS_TYPE = new TypeToken<Set<PluginClass>>() { }.getType();

  private final ArtifactRepository artifactRepository;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CapabilityReader capabilityReader;
  private final File tmpDir;

  @Inject
  ArtifactHttpHandler(CConfiguration cConf, ArtifactRepository artifactRepository,
                      NamespaceQueryAdmin namespaceQueryAdmin, CapabilityReader capabilityReader) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.artifactRepository = artifactRepository;
    this.capabilityReader = capabilityReader;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  @POST
  @Path("/namespaces/system/artifacts")
  public void refreshSystemArtifacts(HttpRequest request, HttpResponder responder) throws Exception {
    try {
      artifactRepository.addSystemArtifacts();
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IOException e) {
      LOG.error("Error while refreshing system artifacts.", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "There was an IO error while refreshing system artifacts, please try again.");
    } catch (WriteConflictException e) {
      LOG.error("Error while refreshing system artifacts.", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @POST
  @Path("/namespaces/{namespace-id}/artifactproperties")
  public void getArtifactProperties(FullHttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @QueryParam("order") @DefaultValue("DESC") String order) throws Exception {

    NamespaceId namespace = validateAndGetNamespace(namespaceId);
    ArtifactSortOrder sortOrder = ArtifactSortOrder.valueOf(order);

    List<ArtifactPropertiesRequest> propertyRequests;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      propertyRequests = GSON.fromJson(reader, BATCH_ARTIFACT_PROPERTIES_REQUEST);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Unable to parse request: " + e.getMessage(), e);
    }

    List<ArtifactSummaryProperties> result = new ArrayList<>(propertyRequests.size());
    for (ArtifactPropertiesRequest propertiesRequest : propertyRequests) {
      NamespaceId requestNamespace =
        propertiesRequest.getScope() == ArtifactScope.SYSTEM ? NamespaceId.SYSTEM : namespace;
      ArtifactRange range = new ArtifactRange(requestNamespace.getNamespace(), propertiesRequest.getName(),
                                              ArtifactVersionRange.parse(propertiesRequest.getVersion()));

      List<ArtifactDetail> artifactDetails = artifactRepository.getArtifactDetails(range, 1, sortOrder);
      for (ArtifactDetail artifactDetail : artifactDetails) {
        Map<String, String> properties = artifactDetail.getMeta().getProperties();
        Map<String, String> filteredProperties = new HashMap<>(propertiesRequest.getProperties().size());
        for (String propertyKey : propertiesRequest.getProperties()) {
          if (properties.containsKey(propertyKey)) {
            filteredProperties.put(propertyKey, properties.get(propertyKey));
          }
        }
        String artifactVersion = artifactDetail.getDescriptor().getArtifactId().getVersion().getVersion();
        result.add(new ArtifactSummaryProperties(propertiesRequest.getName(), artifactVersion,
                                                 propertiesRequest.getScope(), filteredProperties));
      }
    }

    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(result, BATCH_ARTIFACT_PROPERTIES_RESPONSE));
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts")
  public void getArtifacts(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @Nullable @QueryParam("scope") String scope)
    throws Exception {

    try {
      if (scope == null) {
        NamespaceId namespace = validateAndGetNamespace(namespaceId);
        responder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(artifactRepository.getArtifactSummaries(namespace, true)));
      } else {
        NamespaceId namespace = validateAndGetScopedNamespace(Ids.namespace(namespaceId), scope);
        responder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(artifactRepository.getArtifactSummaries(namespace, false)));
      }
    } catch (IOException e) {
      LOG.error("Exception reading artifact metadata for namespace {} from the store.", namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error reading artifact metadata from the store.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}")
  public void getArtifactVersions(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("artifact-name") String artifactName,
                                  @QueryParam("scope") @DefaultValue("user") String scope,
                                  @QueryParam("artifactVersion") String versionRange,
                                  @QueryParam("limit") @DefaultValue("2147483647") String limit,
                                  @QueryParam("order") @DefaultValue("UNORDERED") String order)
    throws Exception {

    NamespaceId namespace = validateAndGetScopedNamespace(Ids.namespace(namespaceId), scope);

    ArtifactRange range = versionRange == null ? null :
      new ArtifactRange(namespaceId, artifactName, ArtifactVersionRange.parse(versionRange));
    int limitNumber = Integer.parseInt(limit);
    limitNumber = limitNumber <= 0 ? Integer.MAX_VALUE : limitNumber;
    ArtifactSortOrder sortOrder = ArtifactSortOrder.valueOf(order);

    try {
      if (range == null) {
        responder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(artifactRepository.getArtifactSummaries(namespace, artifactName,
                                                                               limitNumber, sortOrder)));
      } else {
        responder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(artifactRepository.getArtifactSummaries(range, limitNumber, sortOrder)));
      }
    } catch (IOException e) {
      LOG.error("Exception reading artifacts named {} for namespace {} from the store.", artifactName, namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error reading artifact metadata from the store.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}")
  public void getArtifactInfo(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("artifact-name") String artifactName,
                              @PathParam("artifact-version") String artifactVersion,
                              @QueryParam("scope") @DefaultValue("user") String scope)
    throws Exception {

    NamespaceId namespace = validateAndGetScopedNamespace(Ids.namespace(namespaceId), scope);
    ArtifactId artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      ArtifactDetail detail = artifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId));
      ArtifactDescriptor descriptor = detail.getDescriptor();
      // info hides some fields that are available in detail, such as the location of the artifact
      ArtifactInfo info = new ArtifactInfo(descriptor.getArtifactId(),
                                           detail.getMeta().getClasses(), detail.getMeta().getProperties(),
                                           detail.getMeta().getUsableBy());
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(info, ArtifactInfo.class));
    } catch (IOException e) {
      LOG.error("Exception reading artifacts named {} for namespace {} from the store.", artifactName, namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error reading artifact metadata from the store.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/properties")
  public void getProperties(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("artifact-name") String artifactName,
                            @PathParam("artifact-version") String artifactVersion,
                            @QueryParam("scope") @DefaultValue("user") String scope,
                            @QueryParam("keys") @Nullable String keys)
    throws Exception {

    NamespaceId namespace = validateAndGetScopedNamespace(Ids.namespace(namespaceId), scope);
    ArtifactId artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      ArtifactDetail artifactDetail = artifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId));
      Map<String, String> properties = artifactDetail.getMeta().getProperties();
      Map<String, String> result;

      if (keys != null && !keys.isEmpty()) {
        result = new HashMap<>();
        for (String key : Splitter.on(',').trimResults().split(keys)) {
          result.put(key, properties.get(key));
        }
      } else {
        result = properties;
      }
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(result));
    } catch (IOException e) {
      LOG.error("Exception reading artifacts named {} for namespace {} from the store.", artifactName, namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Error reading artifact properties from the store.");
    }
  }

  @PUT
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void writeProperties(FullHttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("artifact-name") String artifactName,
                              @PathParam("artifact-version") String artifactVersion) throws Exception {
    NamespaceId namespace = NamespaceId.SYSTEM.getNamespace().equalsIgnoreCase(namespaceId) ?
      NamespaceId.SYSTEM : validateAndGetNamespace(namespaceId);
    ArtifactId artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    Map<String, String> properties;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      properties = GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Json Syntax Error while parsing properties from request. " +
                                      "Please check that the properties are a json map from string to string.", e);
    } catch (IOException e) {
      throw new BadRequestException("Unable to read properties from the request.", e);
    }

    try {
      artifactRepository.writeArtifactProperties(Id.Artifact.fromEntityId(artifactId), properties);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IOException e) {
      LOG.error("Exception writing properties for artifact {}.", artifactId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error adding properties to artifact.");
    }
  }

  @PUT
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/properties/{property}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void writeProperty(FullHttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("artifact-name") String artifactName,
                            @PathParam("artifact-version") String artifactVersion,
                            @PathParam("property") String key) throws Exception {
    NamespaceId namespace = NamespaceId.SYSTEM.getNamespace().equalsIgnoreCase(namespaceId) ?
      NamespaceId.SYSTEM : validateAndGetNamespace(namespaceId);
    ArtifactId artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    String value = request.content().toString(StandardCharsets.UTF_8);
    if (value == null) {
      responder.sendStatus(HttpResponseStatus.OK);
      return;
    }

    try {
      artifactRepository.writeArtifactProperty(Id.Artifact.fromEntityId(artifactId), key, value);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IOException e) {
      LOG.error("Exception writing properties for artifact {}.", artifactId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error writing property to artifact.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/properties/{property}")
  public void getProperty(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @PathParam("artifact-name") String artifactName,
                          @PathParam("artifact-version") String artifactVersion,
                          @PathParam("property") String key,
                          @QueryParam("scope") @DefaultValue("user") String scope)
    throws Exception {

    NamespaceId namespace = validateAndGetScopedNamespace(Ids.namespace(namespaceId), scope);
    ArtifactId artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      ArtifactDetail detail = artifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId));
      responder.sendString(HttpResponseStatus.OK, detail.getMeta().getProperties().get(key));
    } catch (IOException e) {
      LOG.error("Exception reading property for artifact {}.", artifactId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error reading properties for artifact.");
    }
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/properties")
  public void deleteProperties(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("artifact-name") String artifactName,
                               @PathParam("artifact-version") String artifactVersion) throws Exception {

    NamespaceId namespace = NamespaceId.SYSTEM.getNamespace().equalsIgnoreCase(namespaceId) ?
      NamespaceId.SYSTEM : validateAndGetNamespace(namespaceId);
    ArtifactId artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      artifactRepository.deleteArtifactProperties(Id.Artifact.fromEntityId(artifactId));
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IOException e) {
      LOG.error("Exception deleting properties for artifact {}.", artifactId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error deleting properties for artifact.");
    }
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/properties/{property}")
  public void deleteProperty(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("artifact-name") String artifactName,
                             @PathParam("artifact-version") String artifactVersion,
                             @PathParam("property") String key) throws Exception {

    NamespaceId namespace = NamespaceId.SYSTEM.getNamespace().equalsIgnoreCase(namespaceId) ?
      NamespaceId.SYSTEM : validateAndGetNamespace(namespaceId);
    ArtifactId artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      artifactRepository.deleteArtifactProperty(Id.Artifact.fromEntityId(artifactId), key);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IOException e) {
      LOG.error("Exception updating properties for artifact {}.", artifactId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error deleting property for artifact.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/extensions")
  public void getArtifactPluginTypes(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("artifact-name") String artifactName,
                                     @PathParam("artifact-version") String artifactVersion,
                                     @QueryParam("scope") @DefaultValue("user") String scope)
    throws NamespaceNotFoundException, BadRequestException, ArtifactNotFoundException {

    NamespaceId namespace = Ids.namespace(namespaceId);
    NamespaceId artifactNamespace = validateAndGetScopedNamespace(namespace, scope);
    ArtifactId artifactId = validateAndGetArtifactId(artifactNamespace, artifactName, artifactVersion);

    try {
      SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins =
        artifactRepository.getPlugins(namespace, Id.Artifact.fromEntityId(artifactId));
      Set<String> pluginTypes = Sets.newHashSet();
      for (Set<PluginClass> pluginClasses : plugins.values()) {
        for (PluginClass pluginClass : pluginClasses) {
          pluginTypes.add(pluginClass.getType());
        }
      }
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(pluginTypes));
    } catch (IOException e) {
      LOG.error("Exception looking up plugins for artifact {}", artifactId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Error reading plugins for the artifact from the store.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/extensions/{plugin-type}")
  public void getArtifactPlugins(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("artifact-name") String artifactName,
                                 @PathParam("artifact-version") String artifactVersion,
                                 @PathParam("plugin-type") String pluginType,
                                 @QueryParam("scope") @DefaultValue("user") String scope)
    throws NamespaceNotFoundException, BadRequestException, ArtifactNotFoundException {

    NamespaceId namespace = Ids.namespace(namespaceId);
    NamespaceId artifactNamespace = validateAndGetScopedNamespace(namespace, scope);
    ArtifactId artifactId = validateAndGetArtifactId(artifactNamespace, artifactName, artifactVersion);

    try {
      SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins =
        artifactRepository.getPlugins(namespace, Id.Artifact.fromEntityId(artifactId), pluginType);
      List<PluginSummary> pluginSummaries = Lists.newArrayList();
      // flatten the map
      for (Map.Entry<ArtifactDescriptor, Set<PluginClass>> pluginsEntry : plugins.entrySet()) {
        ArtifactDescriptor pluginArtifact = pluginsEntry.getKey();
        ArtifactSummary pluginArtifactSummary = ArtifactSummary.from(pluginArtifact.getArtifactId());

        for (PluginClass pluginClass : pluginsEntry.getValue()) {
          try {
            capabilityReader.checkAllEnabled(pluginClass.getRequirements().getCapabilities());
          } catch (CapabilityNotAvailableException e) {
            LOG.debug("Skipping plugin {} because of disabled capability", pluginClass, e);
            continue;
          }
          pluginSummaries.add(new PluginSummary(
            pluginClass.getName(), pluginClass.getType(), pluginClass.getCategory(), pluginClass.getClassName(),
            pluginClass.getRuntimeClassNames(), pluginArtifactSummary, pluginClass.getDescription()));
        }
      }
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(pluginSummaries));
    } catch (IOException e) {
      LOG.error("Exception looking up plugins for artifact {}", artifactId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Error reading plugins for the artifact from the store.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/" +
    "versions/{artifact-version}/extensions/{plugin-type}/plugins/{plugin-name}")
  public void getArtifactPlugin(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("artifact-name") String artifactName,
                                @PathParam("artifact-version") String artifactVersion,
                                @PathParam("plugin-type") String pluginType,
                                @PathParam("plugin-name") String pluginName,
                                @QueryParam("scope") @DefaultValue("user") final String scope,
                                @QueryParam("artifactName") final String pluginArtifactName,
                                @QueryParam("artifactVersion") String pluginVersion,
                                @QueryParam("artifactScope") final String pluginScope,
                                @QueryParam("limit") @DefaultValue("2147483647") String limit,
                                @QueryParam("order") @DefaultValue("UNORDERED") String order)
    throws NamespaceNotFoundException, BadRequestException, ArtifactNotFoundException, InvalidArtifactRangeException {

    NamespaceId namespace = Ids.namespace(namespaceId);
    NamespaceId artifactNamespace = validateAndGetScopedNamespace(namespace, scope);
    final NamespaceId pluginArtifactNamespace = validateAndGetScopedNamespace(namespace, pluginScope);
    ArtifactId parentArtifactId = validateAndGetArtifactId(artifactNamespace, artifactName, artifactVersion);
    final ArtifactVersionRange pluginRange = pluginVersion == null ? null : ArtifactVersionRange.parse(pluginVersion);
    int limitNumber = Integer.parseInt(limit);
    limitNumber = limitNumber <= 0 ? Integer.MAX_VALUE : limitNumber;
    ArtifactSortOrder sortOrder = ArtifactSortOrder.valueOf(order);
    Predicate<ArtifactId> predicate = new Predicate<ArtifactId>() {
      @Override
      public boolean apply(ArtifactId input) {
        // should check if the artifact is from SYSTEM namespace, if not, check if it is from the scoped namespace.
        // by default, the scoped namespace is for USER scope
        return (((pluginScope == null && NamespaceId.SYSTEM.equals(input.getParent()))
          || pluginArtifactNamespace.equals(input.getParent())) &&
          (pluginArtifactName == null || pluginArtifactName.equals(input.getArtifact())) &&
          (pluginRange == null || pluginRange.versionIsInRange(new ArtifactVersion(input.getVersion()))));
      }
    };

    try {
      SortedMap<ArtifactDescriptor, PluginClass> plugins =
        artifactRepository.getPlugins(namespace, Id.Artifact.fromEntityId(parentArtifactId),
                                      pluginType, pluginName, predicate, limitNumber, sortOrder);
      List<PluginInfo> pluginInfos = Lists.newArrayList();

      // flatten the map
      for (Map.Entry<ArtifactDescriptor, PluginClass> pluginsEntry : plugins.entrySet()) {
        ArtifactDescriptor pluginArtifact = pluginsEntry.getKey();
        ArtifactSummary pluginArtifactSummary = ArtifactSummary.from(pluginArtifact.getArtifactId());

        PluginClass pluginClass = pluginsEntry.getValue();
        try {
          capabilityReader.checkAllEnabled(pluginClass.getRequirements().getCapabilities());
        } catch (CapabilityNotAvailableException e) {
          LOG.debug("Skipping plugin {} because of disabled capability", pluginClass, e);
          continue;
        }
        pluginInfos.add(new PluginInfo(pluginClass, pluginArtifactSummary));
      }
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(pluginInfos));
    } catch (PluginNotExistsException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (IOException e) {
      LOG.error("Exception looking up plugins for artifact {}", parentArtifactId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Error reading plugins for the artifact from the store.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/classes/apps")
  public void getApplicationClasses(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @Nullable @QueryParam("scope") String scope)
    throws NamespaceNotFoundException, BadRequestException {

    try {
      if (scope == null) {
        NamespaceId namespace = validateAndGetNamespace(namespaceId);
        responder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(artifactRepository.getApplicationClasses(namespace, true),
                                       APPCLASS_SUMMARIES_TYPE));
      } else {
        NamespaceId namespace = validateAndGetScopedNamespace(Ids.namespace(namespaceId), scope);
        responder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(artifactRepository.getApplicationClasses(namespace, false),
                                       APPCLASS_SUMMARIES_TYPE));
      }
    } catch (IOException e) {
      LOG.error("Error getting app classes for namespace {}.", namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Error reading app class information from store, please try again.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/classes/apps/{classname}")
  public void getApplicationClasses(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("classname") String className,
                                    @QueryParam("scope") @DefaultValue("user") String scope)
    throws NamespaceNotFoundException, BadRequestException {

    NamespaceId namespace = validateAndGetScopedNamespace(Ids.namespace(namespaceId), scope);

    try {
      responder.sendJson(HttpResponseStatus.OK,
                         GSON.toJson(artifactRepository.getApplicationClasses(namespace, className),
                                     APPCLASS_INFOS_TYPE));
    } catch (IOException e) {
      LOG.error("Error getting app classes for namespace {}.", namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Error reading app class information from store, please try again.");
    }
  }

  @POST
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}")
  @AuditPolicy(AuditDetail.HEADERS)
  public BodyConsumer addArtifact(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") final String namespaceId,
                                  @PathParam("artifact-name") final String artifactName,
                                  @HeaderParam(VERSION_HEADER) final String artifactVersion,
                                  @HeaderParam(EXTENDS_HEADER) final String parentArtifactsStr,
                                  @HeaderParam(PLUGINS_HEADER) String pluginClasses)
    throws NamespaceNotFoundException, BadRequestException {

    final NamespaceId namespace = validateAndGetNamespace(namespaceId);

    // if version is explicitly given, validate the id now. otherwise version will be derived from the manifest
    // and validated there
    if (artifactVersion != null && !artifactVersion.isEmpty()) {
      validateAndGetArtifactId(namespace, artifactName, artifactVersion);
    }

    final Set<ArtifactRange> parentArtifacts = parseExtendsHeader(namespace, parentArtifactsStr);

    final Set<PluginClass> additionalPluginClasses;
    if (pluginClasses == null || pluginClasses.isEmpty()) {
      additionalPluginClasses = ImmutableSet.of();
    } else {
      try {
        additionalPluginClasses = GSON.fromJson(pluginClasses, PLUGINS_TYPE);
        additionalPluginClasses.forEach(PluginClass::validate);
      } catch (JsonParseException e) {
        throw new BadRequestException(String.format("%s header '%s' is invalid.", PLUGINS_HEADER, pluginClasses), e);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(String.format("Invalid PluginClasses '%s'.", pluginClasses), e);
      }
    }

    try {
      // copy the artifact contents to local tmp directory
      Files.createDirectories(tmpDir.toPath());
      File destination = File.createTempFile("artifact-", ".jar", tmpDir);

      return new AbstractBodyConsumer(destination) {

        @Override
        protected void onFinish(HttpResponder responder, File uploadedFile) {
          try {
            String version = (artifactVersion == null || artifactVersion.isEmpty()) ?
              getBundleVersion(uploadedFile) : artifactVersion;
            ArtifactId artifactId = validateAndGetArtifactId(namespace, artifactName, version);

            // add the artifact to the repo
            artifactRepository.addArtifact(Id.Artifact.fromEntityId(artifactId),
                                           uploadedFile, parentArtifacts, additionalPluginClasses);
            responder.sendString(HttpResponseStatus.OK, "Artifact added successfully");
          } catch (ArtifactRangeNotFoundException e) {
            responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
          } catch (ArtifactAlreadyExistsException e) {
            responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
          } catch (WriteConflictException e) {
            responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                 "Conflict while writing artifact, please try again.");
          } catch (IOException e) {
            LOG.error("Exception while trying to write artifact {}-{}-{}.",
                      namespaceId, artifactName, artifactVersion, e);
            responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                 "Error performing IO while writing artifact.");
          } catch (BadRequestException e) {
            responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
          } catch (UnauthorizedException e) {
            responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
          } catch (Exception e) {
            LOG.error("Error while writing artifact {}-{}-{}", namespaceId, artifactName, artifactVersion, e);
            responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error while adding artifact.");
          }
        }

        private String getBundleVersion(File file) throws BadRequestException, IOException {
          try (JarFile jarFile = new JarFile(file)) {
            Manifest manifest = jarFile.getManifest();
            if (manifest == null) {
              throw new BadRequestException(
                "Unable to derive version from artifact because it does not contain a manifest. " +
                  "Please package the jar with a manifest, or explicitly specify the artifact version.");
            }
            Attributes attributes = manifest.getMainAttributes();
            String version = attributes == null ? null : attributes.getValue(ManifestFields.BUNDLE_VERSION);
            if (version == null) {
              throw new BadRequestException(
                "Unable to derive version from artifact because manifest does not contain Bundle-Version attribute. " +
                  "Please include Bundle-Version in the manifest, or explicitly specify the artifact version.");
            }
            return version;
          } catch (ZipException e) {
            throw new BadRequestException("Artifact is not in zip format. Please make sure it is a jar file.");
          }
        }

      };
    } catch (IOException e) {
      LOG.error("Exception creating temp file to place artifact {} contents", artifactName, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Server error creating temp file for artifact.");
      return null;
    }
  }

  @DELETE
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}")
  public void deleteArtifact(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("artifact-name") String artifactName,
                             @PathParam("artifact-version") String artifactVersion) throws Exception {
    NamespaceId namespace = NamespaceId.SYSTEM.getNamespace().equalsIgnoreCase(namespaceId) ?
      NamespaceId.SYSTEM : validateAndGetNamespace(namespaceId);
    ArtifactId artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      artifactRepository.deleteArtifact(Id.Artifact.fromEntityId(artifactId));
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IOException e) {
      LOG.error("Exception deleting artifact named {} for namespace {} from the store.", artifactName, namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Error deleting artifact metadata from the store: " + e.getMessage());
    }
  }

  private ArtifactScope validateScope(String scope) throws BadRequestException {
    try {
      return ArtifactScope.valueOf(scope.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Invalid scope " + scope);
    }
  }

  private NamespaceId validateAndGetNamespace(String namespaceId) throws NamespaceNotFoundException {
    return validateAndGetScopedNamespace(Ids.namespace(namespaceId), ArtifactScope.USER);
  }

  private NamespaceId validateAndGetScopedNamespace(NamespaceId namespace, @Nullable String scope)
    throws NamespaceNotFoundException, BadRequestException {
    if (scope != null) {
      return validateAndGetScopedNamespace(namespace, validateScope(scope));
    }
    return validateAndGetScopedNamespace(namespace, ArtifactScope.USER);
  }

  // check that the namespace exists, and check if the request is only supposed to include system artifacts,
  // and returning the system namespace if so.
  private NamespaceId validateAndGetScopedNamespace(NamespaceId namespace, ArtifactScope scope)
    throws NamespaceNotFoundException {

    try {
      namespaceQueryAdmin.get(namespace);
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      // This can only happen when NamespaceAdmin uses HTTP to interact with namespaces.
      // Within AppFabric, NamespaceAdmin is bound to DefaultNamespaceAdmin which directly interacts with MDS.
      // Hence, this should never happen.
      throw Throwables.propagate(e);
    }

    return ArtifactScope.SYSTEM.equals(scope) ? NamespaceId.SYSTEM : namespace;
  }

  private ArtifactId validateAndGetArtifactId(NamespaceId namespace, String name,
                                              String version) throws BadRequestException {
    try {
      return namespace.artifact(name, version);
    } catch (Exception e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  // find out if this artifact extends other artifacts. If so, there will be a header like
  // 'Artifact-Extends: <name>[<lowerversion>,<upperversion>]/<name>[<lowerversion>,<upperversion>]:
  // for example: 'Artifact-Extends: etl-batch[1.0.0,2.0.0]/etl-realtime[1.0.0:3.0.0]
  private Set<ArtifactRange> parseExtendsHeader(NamespaceId namespace, String extendsHeader)
    throws BadRequestException {

    Set<ArtifactRange> parentArtifacts = Sets.newHashSet();

    if (extendsHeader != null && !extendsHeader.isEmpty()) {
      for (String parent : Splitter.on('/').split(extendsHeader)) {
        parent = parent.trim();
        ArtifactRange range;
        // try parsing it as a namespaced range like system:etl-batch[1.0.0,2.0.0)
        try {
          range = ArtifactRanges.parseArtifactRange(parent);
          // only support extending an artifact that is in the same namespace, or system namespace
          if (!NamespaceId.SYSTEM.getNamespace().equals(range.getNamespace()) &&
            !namespace.getNamespace().equals(range.getNamespace())) {
            throw new BadRequestException(
              String.format("Parent artifact %s must be in the same namespace or a system artifact.", parent));
          }
        } catch (InvalidArtifactRangeException e) {
          // if this failed, try parsing as a non-namespaced range like etl-batch[1.0.0,2.0.0)
          try {
            range = ArtifactRanges.parseArtifactRange(namespace.getNamespace(), parent);
          } catch (InvalidArtifactRangeException e1) {
            throw new BadRequestException(String.format("Invalid artifact range %s: %s", parent, e1.getMessage()));
          }
        }
        parentArtifacts.add(range);
      }
    }
    return parentArtifacts;
  }
}

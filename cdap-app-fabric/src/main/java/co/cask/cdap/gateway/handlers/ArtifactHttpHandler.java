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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.ArtifactAlreadyExistsException;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.ArtifactRangeNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.PluginClassDeserializer;
import co.cask.cdap.common.http.AbstractBodyConsumer;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.WriteConflictException;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ApplicationClassInfo;
import co.cask.cdap.proto.artifact.ApplicationClassSummary;
import co.cask.cdap.proto.artifact.ArtifactInfo;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.artifact.InvalidArtifactRangeException;
import co.cask.cdap.proto.artifact.PluginInfo;
import co.cask.cdap.proto.artifact.PluginSummary;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
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
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
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
 * {@link co.cask.http.HttpHandler} for managing adapter lifecycle.
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
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .registerTypeAdapter(PluginClass.class, new PluginClassDeserializer())
    .create();
  private static final Type PLUGINS_TYPE = new TypeToken<Set<PluginClass>>() { }.getType();

  private final ArtifactRepository artifactRepository;
  private final NamespaceAdmin namespaceAdmin;
  private final File tmpDir;

  @Inject
  public ArtifactHttpHandler(CConfiguration cConf,
                             ArtifactRepository artifactRepository, NamespaceAdmin namespaceAdmin) {
    this.namespaceAdmin = namespaceAdmin;
    this.artifactRepository = artifactRepository;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  @POST
  @Path("/namespaces/system/artifacts")
  public void refreshSystemArtifacts(HttpRequest request, HttpResponder responder) {
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

  @GET
  @Path("/namespaces/{namespace-id}/artifacts")
  public void getArtifacts(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @Nullable @QueryParam("scope") String scope)
    throws NamespaceNotFoundException, BadRequestException {

    try {
      if (scope == null) {
        Id.Namespace namespace = validateAndGetNamespace(namespaceId);
        responder.sendJson(HttpResponseStatus.OK, artifactRepository.getArtifacts(namespace, true));
      } else {
        Id.Namespace namespace = validateAndGetScopedNamespace(Id.Namespace.from(namespaceId), scope);
        responder.sendJson(HttpResponseStatus.OK, artifactRepository.getArtifacts(namespace, false));
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
                                  @QueryParam("scope") @DefaultValue("user") String scope)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = validateAndGetScopedNamespace(Id.Namespace.from(namespaceId), scope);

    try {
      responder.sendJson(HttpResponseStatus.OK, artifactRepository.getArtifacts(namespace, artifactName));
    } catch (ArtifactNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Artifacts named " + artifactName + " not found.");
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
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = validateAndGetScopedNamespace(Id.Namespace.from(namespaceId), scope);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      ArtifactDetail detail = artifactRepository.getArtifact(artifactId);
      ArtifactDescriptor descriptor = detail.getDescriptor();
      // info hides some fields that are available in detail, such as the location of the artifact
      ArtifactInfo info = new ArtifactInfo(descriptor.getArtifactId(),
                                           detail.getMeta().getClasses(), detail.getMeta().getProperties());
      responder.sendJson(HttpResponseStatus.OK, info, ArtifactInfo.class, GSON);
    } catch (ArtifactNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Artifact " + artifactId + " not found.");
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
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = validateAndGetScopedNamespace(Id.Namespace.from(namespaceId), scope);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      ArtifactDetail artifactDetail = artifactRepository.getArtifact(artifactId);
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
      responder.sendJson(HttpResponseStatus.OK, result);
    } catch (ArtifactNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Artifact " + artifactId + " not found.");
    } catch (IOException e) {
      LOG.error("Exception reading artifacts named {} for namespace {} from the store.", artifactName, namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Error reading artifact properties from the store.");
    }
  }

  @PUT
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/properties")
  public void writeProperties(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("artifact-name") String artifactName,
                              @PathParam("artifact-version") String artifactVersion)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = Id.Namespace.SYSTEM.getId().equalsIgnoreCase(namespaceId) ?
      Id.Namespace.SYSTEM : validateAndGetNamespace(namespaceId);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    Map<String, String> properties;
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8)) {
      properties = GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Json Syntax Error while parsing properties from request. " +
                                      "Please check that the properties are a json map from string to string.", e);
    } catch (IOException e) {
      throw new BadRequestException("Unable to read properties from the request.", e);
    }

    try {
      artifactRepository.writeArtifactProperties(artifactId, properties);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (ArtifactNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Artifact " + artifactId + " not found.");
    } catch (IOException e) {
      LOG.error("Exception writing properties for artifact {}.", artifactId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error adding properties to artifact.");
    }
  }

  @PUT
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/properties/{property}")
  public void writeProperty(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("artifact-name") String artifactName,
                            @PathParam("artifact-version") String artifactVersion,
                            @PathParam("property") String key)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = Id.Namespace.SYSTEM.getId().equalsIgnoreCase(namespaceId) ?
      Id.Namespace.SYSTEM : validateAndGetNamespace(namespaceId);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    String value = request.getContent().toString(Charsets.UTF_8);
    if (value == null) {
      responder.sendStatus(HttpResponseStatus.OK);
      return;
    }

    try {
      artifactRepository.writeArtifactProperty(artifactId, key, value);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (ArtifactNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Artifact " + artifactId + " not found.");
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
                          @PathParam("property") String key)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = Id.Namespace.SYSTEM.getId().equalsIgnoreCase(namespaceId) ?
      Id.Namespace.SYSTEM : validateAndGetNamespace(namespaceId);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      ArtifactDetail detail = artifactRepository.getArtifact(artifactId);
      responder.sendString(HttpResponseStatus.OK, detail.getMeta().getProperties().get(key));
    } catch (ArtifactNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Artifact " + artifactId + " not found.");
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
                               @PathParam("artifact-version") String artifactVersion)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = Id.Namespace.SYSTEM.getId().equalsIgnoreCase(namespaceId) ?
      Id.Namespace.SYSTEM : validateAndGetNamespace(namespaceId);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      artifactRepository.deleteArtifactProperties(artifactId);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (ArtifactNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Artifact " + artifactId + " not found.");
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
                             @PathParam("property") String key)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = Id.Namespace.SYSTEM.getId().equalsIgnoreCase(namespaceId) ?
      Id.Namespace.SYSTEM : validateAndGetNamespace(namespaceId);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      artifactRepository.deleteArtifactProperty(artifactId, key);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (ArtifactNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Artifact " + artifactId + " not found.");
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

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Id.Namespace artifactNamespace = validateAndGetScopedNamespace(namespace, scope);
    Id.Artifact artifactId = validateAndGetArtifactId(artifactNamespace, artifactName, artifactVersion);

    try {
      SortedMap<ArtifactDescriptor, List<PluginClass>> plugins = artifactRepository.getPlugins(namespace, artifactId);
      Set<String> pluginTypes = Sets.newHashSet();
      for (List<PluginClass> pluginClasses : plugins.values()) {
        for (PluginClass pluginClass : pluginClasses) {
          pluginTypes.add(pluginClass.getType());
        }
      }
      responder.sendJson(HttpResponseStatus.OK, pluginTypes);
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

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Id.Namespace artifactNamespace = validateAndGetScopedNamespace(namespace, scope);
    Id.Artifact artifactId = validateAndGetArtifactId(artifactNamespace, artifactName, artifactVersion);

    try {
      SortedMap<ArtifactDescriptor, List<PluginClass>> plugins =
        artifactRepository.getPlugins(namespace, artifactId, pluginType);
      List<PluginSummary> pluginSummaries = Lists.newArrayList();
      // flatten the map
      for (Map.Entry<ArtifactDescriptor, List<PluginClass>> pluginsEntry : plugins.entrySet()) {
        ArtifactDescriptor pluginArtifact = pluginsEntry.getKey();
        ArtifactSummary pluginArtifactSummary = ArtifactSummary.from(pluginArtifact.getArtifactId());

        for (PluginClass pluginClass : pluginsEntry.getValue()) {
          pluginSummaries.add(new PluginSummary(
            pluginClass.getName(), pluginClass.getType(), pluginClass.getDescription(),
            pluginClass.getClassName(), pluginArtifactSummary));
        }
      }
      responder.sendJson(HttpResponseStatus.OK, pluginSummaries);
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
                                @QueryParam("scope") @DefaultValue("user") String scope)
    throws NamespaceNotFoundException, BadRequestException, ArtifactNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Id.Namespace artifactNamespace = validateAndGetScopedNamespace(namespace, scope);
    Id.Artifact artifactId = validateAndGetArtifactId(artifactNamespace, artifactName, artifactVersion);

    try {
      SortedMap<ArtifactDescriptor, PluginClass> plugins =
        artifactRepository.getPlugins(namespace, artifactId, pluginType, pluginName);
      List<PluginInfo> pluginInfos = Lists.newArrayList();

      // flatten the map
      for (Map.Entry<ArtifactDescriptor, PluginClass> pluginsEntry : plugins.entrySet()) {
        ArtifactDescriptor pluginArtifact = pluginsEntry.getKey();
        ArtifactSummary pluginArtifactSummary = ArtifactSummary.from(pluginArtifact.getArtifactId());

        PluginClass pluginClass = pluginsEntry.getValue();
        pluginInfos.add(new PluginInfo(
          pluginClass.getName(), pluginClass.getType(), pluginClass.getDescription(),
          pluginClass.getClassName(), pluginArtifactSummary, pluginClass.getProperties()));
      }
      responder.sendJson(HttpResponseStatus.OK, pluginInfos);
    } catch (PluginNotExistsException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (IOException e) {
      LOG.error("Exception looking up plugins for artifact {}", artifactId, e);
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
        Id.Namespace namespace = validateAndGetNamespace(namespaceId);
        responder.sendJson(HttpResponseStatus.OK, artifactRepository.getApplicationClasses(namespace, true),
          APPCLASS_SUMMARIES_TYPE, GSON);
      } else {
        Id.Namespace namespace = validateAndGetScopedNamespace(Id.Namespace.from(namespaceId), scope);
        responder.sendJson(HttpResponseStatus.OK, artifactRepository.getApplicationClasses(namespace, false),
          APPCLASS_SUMMARIES_TYPE, GSON);
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

    Id.Namespace namespace = validateAndGetScopedNamespace(Id.Namespace.from(namespaceId), scope);

    try {
      responder.sendJson(HttpResponseStatus.OK, artifactRepository.getApplicationClasses(namespace, className),
        APPCLASS_INFOS_TYPE, GSON);
    } catch (IOException e) {
      LOG.error("Error getting app classes for namespace {}.", namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
        "Error reading app class information from store, please try again.");
    }
  }

  @POST
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}")
  public BodyConsumer addArtifact(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("artifact-name") final String artifactName,
                                  @HeaderParam(VERSION_HEADER) final String artifactVersion,
                                  @HeaderParam(EXTENDS_HEADER) final String parentArtifactsStr,
                                  @HeaderParam(PLUGINS_HEADER) String pluginClasses)
    throws NamespaceNotFoundException, BadRequestException {

    final Id.Namespace namespace = validateAndGetNamespace(namespaceId);

    // if version is explicitly given, validate the id now. otherwise version will be derived from the manifest
    // and validated there
    if (artifactVersion != null && !artifactVersion.isEmpty()) {
      validateAndGetArtifactId(namespace, artifactName, artifactVersion);
    }

    final Set<ArtifactRange> parentArtifacts = parseExtendsHeader(namespace, parentArtifactsStr);

    final Set<PluginClass> additionalPluginClasses;
    if (pluginClasses == null) {
      additionalPluginClasses = ImmutableSet.of();
    } else {
      try {
        additionalPluginClasses = GSON.fromJson(pluginClasses, PLUGINS_TYPE);
      } catch (JsonParseException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, String.format(
          "%s header '%s' is invalid: %s", PLUGINS_HEADER, pluginClasses, e.getMessage()));
        return null;
      }
    }

    try {
      // copy the artifact contents to local tmp directory
      final File destination = File.createTempFile("artifact-", ".jar", tmpDir);

      return new AbstractBodyConsumer(destination) {

        @Override
        protected void onFinish(HttpResponder responder, File uploadedFile) {
          try {
            String version = (artifactVersion == null || artifactVersion.isEmpty()) ?
              getBundleVersion(uploadedFile) : artifactVersion;
            Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, version);

            // add the artifact to the repo
            artifactRepository.addArtifact(artifactId, uploadedFile, parentArtifacts, additionalPluginClasses);
            responder.sendString(HttpResponseStatus.OK, "Artifact added successfully");
          } catch (ArtifactRangeNotFoundException e) {
            responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
          } catch (ArtifactAlreadyExistsException e) {
            responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
          } catch (WriteConflictException e) {
            responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
              "Conflict while writing artifact, please try again.");
          } catch (IOException e) {
            LOG.error("Exception while trying to write artifact {}-{}.", artifactName, artifactVersion, e);
            responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
              "Error performing IO while writing artifact.");
          } catch (BadRequestException e) {
            responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
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
                             @PathParam("artifact-version") String artifactVersion)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = Id.Namespace.SYSTEM.getId().equalsIgnoreCase(namespaceId) ?
      Id.Namespace.SYSTEM : validateAndGetNamespace(namespaceId);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      artifactRepository.deleteArtifact(artifactId);
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

  private Id.Namespace validateAndGetNamespace(String namespaceId) throws NamespaceNotFoundException {
    return validateAndGetScopedNamespace(Id.Namespace.from(namespaceId), ArtifactScope.USER);
  }

  private Id.Namespace validateAndGetScopedNamespace(Id.Namespace namespace, String scope)
    throws NamespaceNotFoundException, BadRequestException {
    if (scope != null) {
      return validateAndGetScopedNamespace(namespace, validateScope(scope));
    }
    return validateAndGetScopedNamespace(namespace, ArtifactScope.USER);
  }

  // check that the namespace exists, and check if the request is only supposed to include system artifacts,
  // and returning the system namespace if so.
  private Id.Namespace validateAndGetScopedNamespace(Id.Namespace namespace, ArtifactScope scope)
    throws NamespaceNotFoundException {

    try {
      namespaceAdmin.get(namespace);
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      // This can only happen when NamespaceAdmin uses HTTP to interact with namespaces.
      // Within AppFabric, NamespaceAdmin is bound to DefaultNamespaceAdmin which directly interacts with MDS.
      // Hence, this should never happen.
      throw Throwables.propagate(e);
    }

    return ArtifactScope.SYSTEM.equals(scope) ? Id.Namespace.SYSTEM : namespace;
  }

  private Id.Artifact validateAndGetArtifactId(Id.Namespace namespace, String name,
                                               String version) throws BadRequestException {
    try {
      return Id.Artifact.from(namespace, name, version);
    } catch (Exception e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  // find out if this artifact extends other artifacts. If so, there will be a header like
  // 'Artifact-Extends: <name>[<lowerversion>,<upperversion>]/<name>[<lowerversion>,<upperversion>]:
  // for example: 'Artifact-Extends: etl-batch[1.0.0,2.0.0]/etl-realtime[1.0.0:3.0.0]
  private Set<ArtifactRange> parseExtendsHeader(Id.Namespace namespace, String extendsHeader)
    throws BadRequestException {

    Set<ArtifactRange> parentArtifacts = Sets.newHashSet();

    if (extendsHeader != null) {
      for (String parent : Splitter.on('/').split(extendsHeader)) {
        parent = parent.trim();
        ArtifactRange range;
        // try parsing it as a namespaced range like system:etl-batch[1.0.0,2.0.0)
        try {
          range = ArtifactRange.parse(parent);
          // only support extending an artifact that is in the same namespace, or system namespace
          if (!range.getNamespace().equals(Id.Namespace.SYSTEM) &&
              !range.getNamespace().equals(namespace)) {
            throw new BadRequestException(
              String.format("Parent artifact %s must be in the same namespace or a system artifact.", parent));
          }
        } catch (InvalidArtifactRangeException e) {
          // if this failed, try parsing as a non-namespaced range like etl-batch[1.0.0,2.0.0)
          try {
            range = ArtifactRange.parse(namespace, parent);
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

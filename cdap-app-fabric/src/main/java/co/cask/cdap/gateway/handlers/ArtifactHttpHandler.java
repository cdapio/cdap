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

import co.cask.cdap.api.artifact.ArtifactDescriptor;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.common.ArtifactAlreadyExistsException;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.ArtifactRangeNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.AbstractBodyConsumer;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.PluginNotExistsException;
import co.cask.cdap.internal.app.runtime.artifact.WriteConflictException;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactInfo;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.artifact.InvalidArtifactRangeException;
import co.cask.cdap.proto.artifact.PluginInfo;
import co.cask.cdap.proto.artifact.PluginSummary;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
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
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final ArtifactRepository artifactRepository;
  private final NamespaceAdmin namespaceAdmin;
  private final File tmpDir;

  @Inject
  public ArtifactHttpHandler(CConfiguration cConf,
                             ArtifactRepository artifactRepository,
                             NamespaceAdmin namespaceAdmin) {
    this.namespaceAdmin = namespaceAdmin;
    this.artifactRepository = artifactRepository;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  @POST
  @Path("/namespaces/system/artifacts")
  public void refreshSystemArtifacts(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId) {
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
                           @QueryParam("includeSystem") @DefaultValue("true") boolean includeSystem)
    throws NamespaceNotFoundException {

    Id.Namespace namespace = validateAndGetNamespace(namespaceId);

    try {
      responder.sendJson(HttpResponseStatus.OK, artifactRepository.getArtifacts(namespace, includeSystem));
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
                                  @QueryParam("isSystem") @DefaultValue("false") boolean isSystem)
    throws NamespaceNotFoundException {

    Id.Namespace namespace = validateAndGetNamespace(namespaceId, isSystem);

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
                              @QueryParam("isSystem") @DefaultValue("false") boolean isSystem)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = validateAndGetNamespace(namespaceId, isSystem);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      ArtifactDetail detail = artifactRepository.getArtifact(artifactId);
      ArtifactDescriptor descriptor = detail.getDescriptor();
      // info hides some fields that are available in detail, such as the location of the artifact
      ArtifactInfo info = new ArtifactInfo(
        descriptor.getName(),
        descriptor.getVersion().getVersion(),
        descriptor.isSystem(),
        detail.getMeta().getClasses());
      responder.sendJson(HttpResponseStatus.OK, info, ArtifactInfo.class, GSON);
    } catch (ArtifactNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Artifact " + artifactId + " not found.");
    } catch (IOException e) {
      LOG.error("Exception reading artifacts named {} for namespace {} from the store.", artifactName, namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error reading artifact metadata from the store.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/extensions")
  public void getArtifactPluginTypes(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("artifact-name") String artifactName,
                                     @PathParam("artifact-version") String artifactVersion,
                                     @QueryParam("isSystem") @DefaultValue("false") boolean isSystem)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = validateAndGetNamespace(namespaceId, isSystem);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      SortedMap<ArtifactDescriptor, List<PluginClass>> plugins = artifactRepository.getPlugins(artifactId);
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
                                 @QueryParam("isSystem") @DefaultValue("false") boolean isSystem)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = validateAndGetNamespace(namespaceId, isSystem);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      SortedMap<ArtifactDescriptor, List<PluginClass>> plugins = artifactRepository.getPlugins(artifactId, pluginType);
      List<PluginSummary> pluginSummaries = Lists.newArrayList();
      // flatten the map
      for (Map.Entry<ArtifactDescriptor, List<PluginClass>> pluginsEntry : plugins.entrySet()) {
        ArtifactDescriptor pluginArtifact = pluginsEntry.getKey();
        ArtifactSummary pluginArtifactSummary = new ArtifactSummary(
          pluginArtifact.getName(), pluginArtifact.getVersion().getVersion(), pluginArtifact.isSystem());

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
                                @QueryParam("isSystem") @DefaultValue("false") boolean isSystem)
    throws NamespaceNotFoundException, BadRequestException {

    Id.Namespace namespace = validateAndGetNamespace(namespaceId);
    Id.Artifact artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);

    try {
      SortedMap<ArtifactDescriptor, PluginClass> plugins =
        artifactRepository.getPlugins(artifactId, pluginType, pluginName);
      List<PluginInfo> pluginInfos = Lists.newArrayList();

      // flatten the map
      for (Map.Entry<ArtifactDescriptor, PluginClass> pluginsEntry : plugins.entrySet()) {
        ArtifactDescriptor pluginArtifact = pluginsEntry.getKey();
        ArtifactSummary pluginArtifactSummary = new ArtifactSummary(
          pluginArtifact.getName(), pluginArtifact.getVersion().getVersion(), pluginArtifact.isSystem());

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
                                    @QueryParam("includeSystem") @DefaultValue("true") boolean includeSystem)
    throws NamespaceNotFoundException {

    Id.Namespace namespace = validateAndGetNamespace(namespaceId);

    // TODO: implement
    responder.sendJson(HttpResponseStatus.OK, Lists.newArrayList());
  }

  @GET
  @Path("/namespaces/{namespace-id}/classes/apps/{classname}")
  public void getApplicationClasses(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("classname") String className,
                                    @QueryParam("includeSystem") @DefaultValue("true") boolean includeSystem)
    throws NamespaceNotFoundException {

    Id.Namespace namespace = validateAndGetNamespace(namespaceId);

    // TODO: implement
    responder.sendJson(HttpResponseStatus.OK, Lists.newArrayList());
  }

  @POST
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}")
  public BodyConsumer addArtifact(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("artifact-name") String artifactName,
                                  @HeaderParam(VERSION_HEADER) String artifactVersion,
                                  @HeaderParam(EXTENDS_HEADER) final String parentArtifactsStr) {

    Id.Namespace namespace;
    try {
      namespace = validateAndGetNamespace(namespaceId);
    } catch (NamespaceNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
      return null;
    }

    final Id.Artifact artifactId;
    try {
      artifactId = validateAndGetArtifactId(namespace, artifactName, artifactVersion);
    } catch (BadRequestException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      return null;
    }

    final Set<ArtifactRange> parentArtifacts;
    try {
      parentArtifacts = parseExtendsHeader(namespace, parentArtifactsStr);
    } catch (BadRequestException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      return null;
    }

    try {
      // copy the artifact contents to local tmp directory
      final File destination = File.createTempFile("artifact-", ".jar", tmpDir);

      return new AbstractBodyConsumer(destination) {

        @Override
        protected void onFinish(HttpResponder responder, File uploadedFile) {
          try {
            // add the artifact to the repo
            artifactRepository.addArtifact(artifactId, uploadedFile, parentArtifacts);
            responder.sendString(HttpResponseStatus.OK, "Artifact added successfully");
          } catch (InvalidArtifactException e) {
            responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
          } catch (ArtifactRangeNotFoundException e) {
            responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
          } catch (ArtifactAlreadyExistsException e) {
            responder.sendString(HttpResponseStatus.CONFLICT, "Artifact " + artifactId + " already exists.");
          } catch (WriteConflictException e) {
            responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
              "Conflict while writing artifact, please try again");
          } catch (IOException e) {
            LOG.error("Exception while trying to write artifact {}.", artifactId, e);
            responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
              "Error performing IO while writing artifact");
          }
        }
      };
    } catch (IOException e) {
      LOG.error("Exception creating temp file to place artifact {} contents", artifactId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Server error creating temp file for artifact.");
      return null;
    }
  }

  private Id.Namespace validateAndGetNamespace(String namespaceId) throws NamespaceNotFoundException {
    return validateAndGetNamespace(namespaceId, false);
  }

  // check that the namespace exists, and check if the request is only supposed to include system artifacts,
  // and returning the system namespace if so.
  private Id.Namespace validateAndGetNamespace(String namespaceId, boolean isSystem) throws NamespaceNotFoundException {
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    if (!namespaceAdmin.hasNamespace(namespace)) {
      throw new NamespaceNotFoundException(namespace);
    }

    return isSystem ? Id.Namespace.SYSTEM : namespace;
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

/*
 * Copyright © 2020 Cask Data, Inc.
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
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.api.artifact.InvalidArtifactRangeException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.artifact.PluginInfo;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.Ids;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link io.cdap.http.HttpHandler} for managing artifacts.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class ArtifactHttpHandlerInternal extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Type PLUGINS_TYPE = new TypeToken<Set<PluginClass>>() { }.getType();
  private static final Type ARTIFACT_INFO_LIST_TYPE = new TypeToken<List<ArtifactInfo>>() { }.getType();

  private final ArtifactRepository artifactRepository;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final File tmpDir;

  @Inject
  ArtifactHttpHandlerInternal(CConfiguration cConf, ArtifactRepository artifactRepository,
                              NamespaceQueryAdmin namespaceQueryAdmin) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.artifactRepository = artifactRepository;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts")
  public void listArtifactsInternal(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId) {
    try {
      List<ArtifactInfo> artifactInfoList = new ArrayList<>();
      artifactInfoList.addAll(artifactRepository.getArtifactsInfo(new NamespaceId(namespaceId)));
      artifactInfoList.addAll(artifactRepository.getArtifactsInfo(NamespaceId.SYSTEM));
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(artifactInfoList, ARTIFACT_INFO_LIST_TYPE));
    } catch (Exception e) {
      LOG.warn("Exception reading artifact metadata for namespace {} from the store.", namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error reading artifact metadata from the store.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}/location")
  public void getArtifactLocation(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @PathParam("artifact-name") String artifactName,
                                  @PathParam("artifact-version") String artifactVersion) {
    try {
      ArtifactDetail artifactDetail = artifactRepository.getArtifact(
        Id.Artifact.from(Id.Namespace.from(namespaceId), artifactName, artifactVersion));
      LOG.debug("wyzhang: ArtifactHttpHandler::getArtifactLocation() got detail URI " + artifactDetail.getDescriptor().getLocation().toURI());
      responder.sendString(HttpResponseStatus.OK, artifactDetail.getDescriptor().getLocation().toURI().getPath());
    } catch (Exception e) {
      LOG.warn("Exception reading artifact metadata for namespace {} from the store.", namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Error reading artifact metadata from the store.");
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}")
  public void getArtifactInfo(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespace,
                              @PathParam("artifact-name") String artifactName,
                              @PathParam("artifact-version") String artifactVersion,
                              @QueryParam("scope") @DefaultValue("user") String scope) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceId.equals(NamespaceId.SYSTEM)) {
      if (!namespaceQueryAdmin.exists(namespaceId)) {
        throw new NamespaceNotFoundException(namespaceId);
      }
    }
    ArtifactId artifactId = new ArtifactId(namespace, artifactName, artifactVersion);
    ArtifactDetail artifactDetail = artifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId));
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(artifactDetail));
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
    LOG.debug("wyzhang: ArtifactHttpHnadlerInternal::getArtifactPlugin() start");
    LOG.debug(String.format(
      "namespaceid %s artifactName %s artifactVersion %s pluginType %s pluginName %s scope %s pluginArtifactName %s pluginVersion %s pluingscope %s",
      namespaceId, artifactName, artifactVersion, pluginType, pluginName, pluginName, scope, pluginArtifactName, pluginScope));
    NamespaceId namespace = Ids.namespace(namespaceId);
    LOG.debug("wyzhang: ArtifactHttpHnadlerInternal::getArtifactPlugin() namespace = " + namespace.toString());
    NamespaceId artifactNamespace = validateAndGetScopedNamespace(namespace, scope);
    LOG.debug("wyzhang: ArtifactHttpHnadlerInternal::getArtifactPlugin() artifact namespace = " + artifactNamespace.toString());
    final NamespaceId pluginArtifactNamespace = validateAndGetScopedNamespace(namespace, pluginScope);
    LOG.debug("wyzhang: ArtifactHttpHnadlerInternal::getArtifactPlugin() plugin artifact namespace = " + pluginArtifactNamespace.toString());
    ArtifactId parentArtifactId = validateAndGetArtifactId(artifactNamespace, artifactName, artifactVersion);
    LOG.debug("wyzhang: ArtifactHttpHnadlerInternal::getArtifactPlugin() parent artifact id = " + parentArtifactId.toString());
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
}

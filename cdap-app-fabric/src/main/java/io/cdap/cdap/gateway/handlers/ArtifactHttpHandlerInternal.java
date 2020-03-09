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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactPropertiesRequest;
import io.cdap.cdap.proto.artifact.ArtifactSummaryProperties;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Type PLUGINS_TYPE = new TypeToken<Set<PluginClass>>() { }.getType();

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
}

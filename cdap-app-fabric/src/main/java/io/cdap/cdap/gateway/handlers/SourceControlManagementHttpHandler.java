/*
 * Copyright © 2023 Cask Data, Inc.
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
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ForbiddenException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.services.SourceControlManagementService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RemoteRepositoryValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigRequest;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.SetRepositoryResponse;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link io.cdap.http.HttpHandler} for source control management.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/repository")
public class SourceControlManagementHttpHandler extends AbstractAppFabricHttpHandler {
  private final SourceControlManagementService sourceControlService;
  private final FeatureFlagsProvider featureFlagsProvider;
  private static final Gson GSON = new Gson();

  @Inject
  SourceControlManagementHttpHandler(CConfiguration configuration,
                                     SourceControlManagementService sourceControlService) {
    this.sourceControlService = sourceControlService;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(configuration);
  }

  @PUT
  @Path("/")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void setRepository(FullHttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {
    checkSourceControlFeatureFlag();
    NamespaceId namespace = validateNamespaceId(namespaceId);
    
    try {
      RepositoryConfigRequest repoRequest = validateAndGetRepoConfig(request);
      if (repoRequest.shouldTest()) {
        sourceControlService.validateRepository(namespace, repoRequest.getConfig());
        responder.sendStatus(HttpResponseStatus.OK);
        return;
      }

      RepositoryMeta repoMeta = sourceControlService.setRepository(namespace, repoRequest.getConfig());
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(repoMeta));
    } catch (RepositoryConfigValidationException e) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, GSON.toJson(new SetRepositoryResponse(e)));
    } catch (RemoteRepositoryValidationException e) {
      responder.sendJson(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                         GSON.toJson(new SetRepositoryResponse(e.getMessage())));
    }
  }

  @GET
  @Path("/")
  public void getRepository(FullHttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {
    checkSourceControlFeatureFlag();
    NamespaceId namespace = validateNamespaceId(namespaceId);
    RepositoryMeta repoMeta = sourceControlService.getRepositoryMeta(namespace);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(repoMeta));
  }

  @DELETE
  @Path("/")
  public void deleteRepository(FullHttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId) throws Exception {
    checkSourceControlFeatureFlag();
    sourceControlService.deleteRepository(validateNamespaceId(namespaceId));
    responder.sendString(HttpResponseStatus.OK, String.format("Deleted repository configuration for namespace '%s'.",
                                                              namespaceId));
  }

  /**
   *
   * throws {@link ForbiddenException} if the feature is disabled
   */
  private void checkSourceControlFeatureFlag() throws ForbiddenException {
    if (!Feature.SOURCE_CONTROL_MANAGEMENT_GIT.isEnabled(featureFlagsProvider)) {
      throw new ForbiddenException("Source Control Management feature is not enabled.");
    }
  }

  private NamespaceId validateNamespaceId(String namespaceId) throws BadRequestException {
    try {
      return new NamespaceId(namespaceId);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
  }

  private RepositoryConfigRequest validateAndGetRepoConfig(FullHttpRequest request) throws BadRequestException {
    try {
      RepositoryConfigRequest repoRequest = parseBody(request, RepositoryConfigRequest.class);
      if (repoRequest == null || repoRequest.getConfig() == null) {
        throw new RepositoryConfigValidationException("Repository configuration must be specified.");
      }
      repoRequest.getConfig().validate();
      return repoRequest;
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid request body: " + e.getMessage(), e);
    }
  }
}

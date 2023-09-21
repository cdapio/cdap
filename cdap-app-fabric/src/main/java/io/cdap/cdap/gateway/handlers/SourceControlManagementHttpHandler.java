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

import com.google.common.base.Strings;
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
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.operationrun.OperationRun;
import io.cdap.cdap.proto.sourcecontrol.PushAppRequest;
import io.cdap.cdap.proto.sourcecontrol.RemoteRepositoryValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigRequest;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.SetRepositoryResponse;
import io.cdap.cdap.sourcecontrol.NoChangesToPullException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.operationrunner.PullMultipleAppsRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.PushMultipleAppsRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.RepositoryAppsResponse;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
  SourceControlManagementHttpHandler(CConfiguration cConf,
                                     SourceControlManagementService sourceControlService) {
    this.sourceControlService = sourceControlService;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
  }

  /**
   * Updates or validates a repository configuration.
   */
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

  /**
   * Gets a repository configuration that has last update time.
   */
  @GET
  @Path("/")
  public void getRepository(FullHttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {
    checkSourceControlFeatureFlag();
    NamespaceId namespace = validateNamespaceId(namespaceId);
    RepositoryMeta repoMeta = sourceControlService.getRepositoryMeta(namespace);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(repoMeta));
  }

  /**
   * Deletes a repository configuration.
   */
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
   * Lists applications found in linked repository for the given namespace.
   * Returns the application name and git-filehash for each application config file found
   * <pre>
   * {
   *  apps:[
   *    {
   *      "name": "xyz",
   *      "fileHash": "abc"
   *    }
   *  ]
   * }
   * </pre>
   */
  @GET
  @Path("/apps")
  public void listApplications(FullHttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId) throws Exception {
    checkSourceControlFeatureFlag();
    NamespaceId namespace = validateNamespaceId(namespaceId);
    RepositoryAppsResponse listResponse = sourceControlService.listApps(namespace);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(listResponse));
  }

  /**
   * Pushes an application configs of the latest version to linked repository in Json format. It expects a post body
   * that has an optional commit message
   * E.g.
   *
   * <pre>
   * {@code
   * {
   *   "commitMessage": "pushed application XYZ"
   * }
   * }
   *
   * </pre>
   * The response will a {@link PushAppResponse} object, which encapsulates the application name,
   * version and fileHash.
   */
  @POST
  @Path("/apps/{app-id}/push")
  public void pushApp(FullHttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("app-id") String appId) throws Exception {
    checkSourceControlFeatureFlag();
    ApplicationReference appRef = validateAppReference(namespaceId, appId);
    PushAppRequest appsRequest = validateAndGetAppsRequest(request);

    try {
      PushAppResponse pushResponse = sourceControlService.pushApp(appRef, appsRequest.getCommitMessage());
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(pushResponse));
    } catch (NoChangesToPushException e) {
      responder.sendString(HttpResponseStatus.OK, e.getMessage());
    }
  }

  /**
   * Pull the requested application from linked repository and deploy in current namespace.
   */
  @POST
  @Path("/apps/{app-id}/pull")
  public void pullApp(FullHttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("app-id") final String appId) throws Exception {
    checkSourceControlFeatureFlag();
    ApplicationReference appRef = validateAppReference(namespaceId, appId);

    try {
      ApplicationRecord appRecord = sourceControlService.pullAndDeploy(appRef);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(appRecord));
    } catch (NoChangesToPullException e) {
      responder.sendString(HttpResponseStatus.OK, e.getMessage());
    }
  }


  /**
   * Push the requested applications to the linked repository.
   *
   * The response will be a {@link OperationRun} object, which encapsulates the
   * long-running operation initiated to push the applications. The client can poll the
   * operations api to get the status of the push operation.
   */
  @POST
  @Path("/apps/push")
  public void pushApps(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId) throws Exception {
    checkSourceControlFeatureFlag();
    PushMultipleAppsRequest pushMultiRequest;
    try {
      pushMultiRequest = parseBody(request, PushMultipleAppsRequest.class);
    } catch (JsonSyntaxException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid request body: " + e.getMessage());
      return;
    }

    if (null == pushMultiRequest) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid request body.");
      return;
    }

    if (pushMultiRequest.getAppIds().isEmpty()) {
      responder.sendString(HttpResponseStatus.OK, "No apps to push.");
    }

    List<ApplicationReference> appRefs = new ArrayList<>();
    for (String appId: pushMultiRequest.getAppIds()) {
      ApplicationReference appRef = validateAppReference(namespaceId, appId);
      appRefs.add(appRef);
    }

    try {
      OperationRun operationResponse = sourceControlService.pushMulti(
          new NamespaceId(namespaceId),
          appRefs,
          pushMultiRequest.getCommitMessage()
      );
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(operationResponse));
    } catch (NoChangesToPullException e) {
      responder.sendString(HttpResponseStatus.OK, e.getMessage());
    }
  }

  /**
   * Pull the requested applications from linked repository and deploy them in the current namespace.
   *
   * The response will be a {@link OperationRun} object, which encapsulates the
   * long-running operation initiated to pull and deploy the applications. The client can poll the
   * operations api to get the status of the pull operation.
   */
  @POST
  @Path("/apps/pull")
  public void pullApps(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId) throws Exception {
    checkSourceControlFeatureFlag();
    PullMultipleAppsRequest pullMultiRequest;
    try {
      pullMultiRequest = parseBody(request, PullMultipleAppsRequest.class);
    } catch (JsonSyntaxException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid request body: " + e.getMessage());
      return;
    }

    if (null == pullMultiRequest) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid request body.");
      return;
    }

    if (pullMultiRequest.getAppIds().isEmpty()) {
      responder.sendString(HttpResponseStatus.OK, "No apps to pull.");
    }

    List<ApplicationReference> appRefs = new ArrayList<>();
    for (String appId: pullMultiRequest.getAppIds()) {
      ApplicationReference appRef = validateAppReference(namespaceId, appId);
      appRefs.add(appRef);
    }

    try {
      OperationRun operationResponse = sourceControlService.pullAndDeployMulti(
          new NamespaceId(namespaceId),
          appRefs
      );
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(operationResponse));
    } catch (NoChangesToPullException e) {
      responder.sendString(HttpResponseStatus.OK, e.getMessage());
    }
  }

  private PushAppRequest validateAndGetAppsRequest(FullHttpRequest request) throws BadRequestException {
    PushAppRequest appRequest;
    try {
      appRequest = parseBody(request, PushAppRequest.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid request body: " + e.getMessage());
    }

    if (appRequest == null || Strings.isNullOrEmpty(appRequest.getCommitMessage())) {
      throw new BadRequestException("Please specify commit message in the request body.");
    }

    return appRequest;
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

  private ApplicationReference validateAppReference(String namespaceId, String appName) throws BadRequestException {
    try {
      return new NamespaceId(namespaceId).appReference(appName);
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

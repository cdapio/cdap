/*
 * Copyright © 2014-2019 Cask Data, Inc.
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


import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.AbstractBodyConsumer;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.WriteConflictException;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.BatchApplicationDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
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
 * {@link io.cdap.http.HttpHandler} for managing application lifecycle.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppLifecycleHttpHandler extends AbstractAppFabricHttpHandler {
  // Gson for writing response
  private static final Gson GSON = new Gson();
  // Gson for decoding request
  private static final Gson DECODE_GSON = new GsonBuilder()
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .create();
  private static final Logger LOG = LoggerFactory.getLogger(AppLifecycleHttpHandler.class);

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  private final CConfiguration configuration;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final NamespacePathLocator namespacePathLocator;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final File tmpDir;

  @Inject
  AppLifecycleHttpHandler(CConfiguration configuration,
                          ProgramRuntimeService runtimeService,
                          NamespaceQueryAdmin namespaceQueryAdmin,
                          NamespacePathLocator namespacePathLocator,
                          ApplicationLifecycleService applicationLifecycleService) {
    this.configuration = configuration;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.runtimeService = runtimeService;
    this.namespacePathLocator = namespacePathLocator;
    this.applicationLifecycleService = applicationLifecycleService;
    this.tmpDir = new File(new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR)),
                           configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  /**
   * Creates an application with the specified name from an artifact.
   */
  @PUT
  @Path("/apps/{app-id}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public BodyConsumer create(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId,
                             @PathParam("app-id") final String appId)
    throws BadRequestException, NamespaceNotFoundException {

    ApplicationId applicationId = validateApplicationId(namespaceId, appId);

    try {
      return deployAppFromArtifact(applicationId);
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Deploy failed: {}" + ex.getMessage());
      return null;
    }
  }

  /**
   * Deploys an application.
   */
  @POST
  @Path("/apps")
  @AuditPolicy({AuditDetail.RESPONSE_BODY, AuditDetail.HEADERS})
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId,
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName,
                             @HeaderParam(APP_CONFIG_HEADER) String configString,
                             @HeaderParam(PRINCIPAL_HEADER) String ownerPrincipal,
                             @DefaultValue("true") @HeaderParam(SCHEDULES_HEADER) boolean updateSchedules)
    throws BadRequestException, NamespaceNotFoundException {

    NamespaceId namespace = validateNamespace(namespaceId);
    // null means use name provided by app spec
    try {
      return deployApplication(responder, namespace, null, archiveName, configString, ownerPrincipal, updateSchedules);
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Deploy failed: " + ex.getMessage());
      return null;
    }
  }

  /**
   * Creates an application with the specified name and app-id from an artifact.
   */
  @POST
  @Path("/apps/{app-id}/versions/{version-id}/create")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public BodyConsumer createAppVersion(HttpRequest request, HttpResponder responder,
                                       @PathParam("namespace-id") final String namespaceId,
                                       @PathParam("app-id") final String appId,
                                       @PathParam("version-id") final String versionId)
    throws Exception {

    ApplicationId applicationId = validateApplicationVersionId(namespaceId, appId, versionId);

    if (!applicationLifecycleService.updateAppAllowed(applicationId)) {
      responder.sendString(HttpResponseStatus.CONFLICT,
                           String.format("Cannot update the application because version %s already exists", versionId));
    }
    try {
      return deployAppFromArtifact(applicationId);
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Deploy failed: " + ex.getMessage());
      return null;
    }
  }

  /**
   * Returns a list of applications associated with a namespace.
   */
  @GET
  @Path("/apps")
  public void getAllApps(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @QueryParam("artifactName") String artifactName,
                         @QueryParam("artifactVersion") String artifactVersion)
    throws Exception {

    NamespaceId namespace = validateNamespace(namespaceId);

    Set<String> names = new HashSet<>();
    if (!Strings.isNullOrEmpty(artifactName)) {
      for (String name : Splitter.on(',').split(artifactName)) {
        names.add(name);
      }
    }
    List<ApplicationRecord> apps = applicationLifecycleService.getApps(namespace, names, artifactVersion)
      .stream()
      .map(ApplicationRecord::new)
      .collect(Collectors.toList());

    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(apps));
  }

  /**
   * Returns the info associated with the application.
   */
  @GET
  @Path("/apps/{app-id}")
  public void getAppInfo(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") final String namespaceId,
                         @PathParam("app-id") final String appId)
    throws Exception {

    ApplicationId applicationId = validateApplicationId(namespaceId, appId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(applicationLifecycleService.getAppDetail(applicationId)));
  }

  /**
   * Returns the list of versions of the application.
   */
  @GET
  @Path("/apps/{app-id}/versions")
  public void listAppVersions(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") final String namespaceId,
                              @PathParam("app-id") final String appId) throws Exception {
    ApplicationId applicationId = validateApplicationId(namespaceId, appId);
    Collection<String> versions = applicationLifecycleService.getAppVersions(namespaceId, appId);
    if (versions.isEmpty()) {
      throw new ApplicationNotFoundException(applicationId);
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(versions));
  }

  /**
   * Returns the info associated with the application given appId and appVersion.
   */
  @GET
  @Path("/apps/{app-id}/versions/{version-id}")
  public void getAppVersionInfo(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") final String namespaceId,
                                @PathParam("app-id") final String appId,
                                @PathParam("version-id") final String versionId)
    throws Exception {

    ApplicationId applicationId = validateApplicationVersionId(namespaceId, appId, versionId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(applicationLifecycleService.getAppDetail(applicationId)));
  }

  /**
   * Returns the plugins in the application.
   */
  @GET
  @Path("/apps/{app-id}/plugins")
  public void getPluginsInfo(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId,
                             @PathParam("app-id") final String appId)
    throws NamespaceNotFoundException, BadRequestException, ApplicationNotFoundException {

    ApplicationId applicationId = validateApplicationId(namespaceId, appId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(applicationLifecycleService.getPlugins(applicationId)));
  }

  /**
   * Delete an application specified by appId.
   */
  @DELETE
  @Path("/apps/{app-id}")
  public void deleteApp(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-id") final String appId) throws Exception {
    ApplicationId id = validateApplicationId(namespaceId, appId);
    applicationLifecycleService.removeApplication(id);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Delete an application specified by appId and versionId.
   */
  @DELETE
  @Path("/apps/{app-id}/versions/{version-id}")
  public void deleteAppVersion(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") final String namespaceId,
                               @PathParam("app-id") final String appId,
                               @PathParam("version-id") final String versionId) throws Exception {
    ApplicationId id = validateApplicationVersionId(namespaceId, appId, versionId);
    applicationLifecycleService.removeApplication(id);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Deletes all applications in CDAP.
   */
  @DELETE
  @Path("/apps")
  public void deleteAllApps(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {
    NamespaceId id = validateNamespace(namespaceId);
    applicationLifecycleService.removeAll(id);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Updates an existing application.
   */
  @POST
  @Path("/apps/{app-id}/update")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateApp(FullHttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") final String namespaceId,
                        @PathParam("app-id") final String appName)
    throws NotFoundException, BadRequestException, UnauthorizedException, IOException {

    ApplicationId appId = validateApplicationId(namespaceId, appName);

    AppRequest appRequest;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      appRequest = DECODE_GSON.fromJson(reader, AppRequest.class);
    } catch (IOException e) {
      LOG.error("Error reading request to update app {} in namespace {}.", appName, namespaceId, e);
      throw new IOException("Error reading request body.");
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }

    try {
      applicationLifecycleService.updateApp(appId, appRequest, createProgramTerminator());
      responder.sendString(HttpResponseStatus.OK, "Update complete.");
    } catch (InvalidArtifactException e) {
      throw new BadRequestException(e.getMessage());
    } catch (ConflictException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
    } catch (NotFoundException | UnauthorizedException e) {
      throw e;
    } catch (Exception e) {
      // this is the same behavior as deploy app pipeline, but this is bad behavior. Error handling needs improvement.
      LOG.error("Deploy failure", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }
  }

  /**
   * Gets {@link ApplicationDetail} for a set of applications. It expects a post body as a array of object, with each
   * object specifying the applciation id and an optional version. E.g.
   *
   * <pre>
   * {@code
   * [
   *   {"appId":"XYZ", "version":"1.2.3"},
   *   {"appId":"ABC"},
   *   {"appId":"FOO", "version":"2.3.4"},
   * ]
   * }
   * </pre>
   * The response will be an array of {@link BatchApplicationDetail} object, which either indicates a success (200) or
   * failure for each of the requested application in the same order as the request.
   */
  @POST
  @Path("/appdetail")
  public void getApplicationDetails(FullHttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespace) throws Exception {

    List<ApplicationId> appIds = decodeAndValidateBatchApplication(validateNamespace(namespace), request);
    Map<ApplicationId, ApplicationDetail> details = applicationLifecycleService.getAppDetails(appIds);

    List<BatchApplicationDetail> result = new ArrayList<>();
    for (ApplicationId appId : appIds) {
      ApplicationDetail detail = details.get(appId);
      if (detail == null) {
        result.add(new BatchApplicationDetail(new NotFoundException(appId)));
      } else {
        result.add(new BatchApplicationDetail(detail));
      }
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(result));
  }

  /**
   * Decodes request coming from the {@link #getApplicationDetails(FullHttpRequest, HttpResponder, String)} call.
   */
  private List<ApplicationId> decodeAndValidateBatchApplication(NamespaceId namespaceId,
                                                                FullHttpRequest request) throws BadRequestException {
    try {
      List<ApplicationId> result = new ArrayList<>();
      JsonArray array = DECODE_GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), JsonArray.class);
      if (array == null) {
        throw new BadRequestException("Request body is invalid json, please check that it is a json array.");
      }
      for (JsonElement element : array) {
        if (!element.isJsonObject()) {
          throw new BadRequestException("Request element is expected to be a json object.");
        }
        JsonObject obj = element.getAsJsonObject();
        if (!obj.has("appId")) {
          throw new BadRequestException("Missing 'appId' in the request element.");
        }
        String appId = obj.get("appId").getAsString();

        JsonElement version = obj.get("version");
        if (version == null) {
          result.add(validateApplicationId(namespaceId, appId));
        } else {
          result.add(validateApplicationVersionId(namespaceId, appId, version.getAsString()));
        }
      }
      return result;
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }
  }

  // normally we wouldn't want to use a body consumer but would just want to read the request body directly
  // since it wont be big. But the deploy app API has one path with different behavior based on content type
  // the other behavior requires a BodyConsumer and only have one method per path is allowed,
  // so we have to use a BodyConsumer
  private BodyConsumer deployAppFromArtifact(final ApplicationId appId) throws IOException {

    // createTempFile() needs a prefix of at least 3 characters
    return new AbstractBodyConsumer(File.createTempFile("apprequest-" + appId, ".json", tmpDir)) {

      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) {
        try (FileReader fileReader = new FileReader(uploadedFile)) {

          AppRequest<?> appRequest = DECODE_GSON.fromJson(fileReader, AppRequest.class);
          ArtifactSummary artifactSummary = appRequest.getArtifact();

          KerberosPrincipalId ownerPrincipalId =
            appRequest.getOwnerPrincipal() == null ? null : new KerberosPrincipalId(appRequest.getOwnerPrincipal());

          // if we don't null check, it gets serialized to "null"
          Object config = appRequest.getConfig();
          String configString = config == null ? null :
            config instanceof String ? (String) config : GSON.toJson(config);

          try {
            applicationLifecycleService.deployApp(appId.getParent(), appId.getApplication(), appId.getVersion(),
                                                  artifactSummary, configString, createProgramTerminator(),
                                                  ownerPrincipalId, appRequest.canUpdateSchedules());
          } catch (DatasetManagementException e) {
            if (e.getCause() instanceof UnauthorizedException) {
              throw (UnauthorizedException) e.getCause();
            } else {
              throw e;
            }
          }
          responder.sendString(HttpResponseStatus.OK, "Deploy Complete");
        } catch (ArtifactNotFoundException e) {
          responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
        } catch (ConflictException e) {
          responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
        } catch (UnauthorizedException e) {
          responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
        } catch (InvalidArtifactException e) {
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        } catch (IOException e) {
          LOG.error("Error reading request body for creating app {}.", appId);
          responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, String.format(
            "Error while reading json request body for app %s.", appId));
        } catch (Exception e) {
          LOG.error("Deploy failure", e);
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        }
      }
    };
  }

  private BodyConsumer deployApplication(final HttpResponder responder,
                                         final NamespaceId namespace,
                                         final String appId,
                                         final String archiveName,
                                         final String configString,
                                         @Nullable final String ownerPrincipal,
                                         final boolean updateSchedules) throws IOException {

    Id.Namespace idNamespace = Id.Namespace.fromEntityId(namespace);
    Location namespaceHomeLocation = namespacePathLocator.get(namespace);
    if (!namespaceHomeLocation.exists()) {
      String msg = String.format("Home directory %s for namespace %s not found",
                                 namespaceHomeLocation, namespace.getNamespace());
      LOG.error(msg);
      responder.sendString(HttpResponseStatus.NOT_FOUND, msg);
      return null;
    }

    if (archiveName == null || archiveName.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format(
                             "%s header not present. Please include the header and set its value to the jar name.",
                             ARCHIVE_NAME_HEADER),
                           new DefaultHttpHeaders().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE));
      return null;
    }

    // TODO: (CDAP-3258) error handling needs to be refactored here, should be able just to throw the exception,
    // but the caller catches all exceptions and responds with a 500
    final Id.Artifact artifactId;
    try {
      artifactId = Id.Artifact.parse(idNamespace, archiveName);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      return null;
    }

    KerberosPrincipalId ownerPrincipalId = ownerPrincipal == null ? null : new KerberosPrincipalId(ownerPrincipal);

    // Store uploaded content to a local temp file
    String namespacesDir = configuration.get(Constants.Namespace.NAMESPACES_DIR);
    File localDataDir = new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR));
    File namespaceBase = new File(localDataDir, namespacesDir);
    File tempDir = new File(new File(namespaceBase, namespace.getNamespace()),
                            configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException("Could not create temporary directory at: " + tempDir);
    }

    final KerberosPrincipalId finalOwnerPrincipalId = ownerPrincipalId;
    return new AbstractBodyConsumer(File.createTempFile("app-", ".jar", tempDir)) {

      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) {
        try {
          // deploy app
          ApplicationWithPrograms app =
            applicationLifecycleService.deployAppAndArtifact(namespace, appId, artifactId, uploadedFile, configString,
                                                             finalOwnerPrincipalId, createProgramTerminator(),
                                                             updateSchedules);
          LOG.info("Successfully deployed app {} in namespace {} from artifact {} with configuration {} and " +
                     "principal {}", app.getApplicationId().getApplication(), namespace.getNamespace(), artifactId,
                   configString, finalOwnerPrincipalId);
          responder.sendString(HttpResponseStatus.OK, String.format("Successfully deployed app %s",
                                                                    app.getApplicationId().getApplication()));
        } catch (InvalidArtifactException e) {
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        } catch (ArtifactAlreadyExistsException e) {
          responder.sendString(HttpResponseStatus.CONFLICT, String.format(
            "Artifact '%s' already exists. Please use the API that creates an application from an existing artifact. " +
              "If you are trying to replace the artifact, please delete it and then try again.", artifactId));
        } catch (WriteConflictException e) {
          // don't really expect this to happen. It means after multiple retries there were still write conflicts.
          LOG.warn("Write conflict while trying to add artifact {}.", artifactId, e);
          responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                               "Write conflict while adding artifact. This can happen if multiple requests to add " +
                                 "the same artifact occur simultaneously. Please try again.");
        } catch (UnauthorizedException e) {
          responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
        } catch (ConflictException e) {
          responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
        } catch (Exception e) {
          LOG.error("Deploy failure", e);
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        }
      }
    };
  }

  private ProgramTerminator createProgramTerminator() {
    return new ProgramTerminator() {
      @Override
      public void stop(ProgramId programId) throws Exception {
        switch (programId.getType()) {
          case SERVICE:
          case WORKER:
            stopProgramIfRunning(programId);
            break;
        }
      }
    };
  }

  private void stopProgramIfRunning(ProgramId programId) throws InterruptedException, ExecutionException {
    ProgramRuntimeService.RuntimeInfo programRunInfo = findRuntimeInfo(programId, runtimeService);
    if (programRunInfo != null) {
      ProgramController controller = programRunInfo.getController();
      controller.stop().get();
    }
  }

  private NamespaceId validateNamespace(String namespaceId) throws BadRequestException, NamespaceNotFoundException {
    NamespaceId namespace;
    if (namespaceId == null) {
      throw new BadRequestException("Path parameter namespace-id cannot be empty");
    }
    if (EntityId.isValidId(namespaceId)) {
      namespace = new NamespaceId(namespaceId);
    } else {
      throw new BadRequestException(String.format("Invalid namespace '%s'", namespaceId));
    }

    try {
      if (!namespace.equals(NamespaceId.SYSTEM)) {
        namespaceQueryAdmin.get(namespace);
      }
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      // This can only happen when NamespaceAdmin uses HTTP calls to interact with namespaces.
      // In AppFabricServer, NamespaceAdmin is bound to DefaultNamespaceAdmin, which interacts directly with the MDS.
      // Hence, this exception will never be thrown
      throw Throwables.propagate(e);
    }
    return namespace;
  }

  private ApplicationId validateApplicationId(String namespace, String appId)
    throws BadRequestException, NamespaceNotFoundException {
    return validateApplicationId(validateNamespace(namespace), appId);
  }

  private ApplicationId validateApplicationId(NamespaceId namespaceId, String appId) throws BadRequestException {
    return validateApplicationVersionId(namespaceId, appId, ApplicationId.DEFAULT_VERSION);
  }

  private ApplicationId validateApplicationVersionId(String namespace, String appId, String versionId)
    throws BadRequestException, NamespaceNotFoundException {
    return validateApplicationVersionId(validateNamespace(namespace), appId, versionId);
  }

  private ApplicationId validateApplicationVersionId(NamespaceId namespaceId, String appId,
                                                     String versionId) throws BadRequestException {
    if (appId == null) {
      throw new BadRequestException("Path parameter app-id cannot be empty");
    }
    if (!EntityId.isValidId(appId)) {
      throw new BadRequestException(String.format("Invalid app name '%s'", appId));
    }
    if (versionId == null) {
      throw new BadRequestException("Path parameter version-id cannot be empty");
    }
    if (EntityId.isValidVersionId(versionId)) {
      return namespaceId.app(appId, versionId);
    }
    throw new BadRequestException(String.format("Invalid version '%s'", versionId));
  }
}

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


import com.google.api.client.json.JsonObjectParser;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.gson.*;
import com.google.gson.stream.JsonWriter;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.ApplicationFilter;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.NotImplementedException;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.http.AbstractBodyConsumer;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.WriteConflictException;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.ApplicationUpdateDetail;
import io.cdap.cdap.proto.BatchApplicationDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.http.BodyConsumer;
import io.cdap.http.ChunkResponder;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
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

import jdk.nashorn.internal.parser.JSONParser;
import org.apache.twill.filesystem.Location;
import scala.util.parsing.json.JSONObject;

/**
 * {@link io.cdap.http.HttpHandler} for managing application lifecycle.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppLifecycleHttpHandler extends AbstractAppLifecycleHttpHandler {
  /**
   * Key in json paginated applications list response.
   */
  public static final String APP_LIST_PAGINATED_KEY = "applications";

  /**
   * Runtime program service for running and managing programs.
   */
  private final NamespacePathLocator namespacePathLocator;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final FeatureFlagsProvider featureFlagsProvider;

  @Inject
  AppLifecycleHttpHandler(CConfiguration configuration,
      ProgramRuntimeService runtimeService,
      NamespaceQueryAdmin namespaceQueryAdmin,
      NamespacePathLocator namespacePathLocator,
      ApplicationLifecycleService applicationLifecycleService,
      AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext) {
    super(configuration, namespaceQueryAdmin, runtimeService, applicationLifecycleService);
    this.namespacePathLocator = namespacePathLocator;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(configuration);
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
      throws BadRequestException, NamespaceNotFoundException, AccessException {
    String versionId = ApplicationId.DEFAULT_VERSION;
    // If LCM flow is enabled - we generate specific versions of the app.
    if (Feature.LIFECYCLE_MANAGEMENT_EDIT.isEnabled(featureFlagsProvider)) {
      versionId = RunIds.generate().getId();
    }
    ApplicationId applicationId = validateApplicationVersionId(namespaceId, appId, versionId);

    try {
      return deployAppFromArtifact(applicationId);
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
          "Deploy failed: " + ex.getMessage());
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
      throws BadRequestException, NamespaceNotFoundException, AccessException {

    NamespaceId namespace = validateNamespace(namespaceId);
    // null means use name provided by app spec
    try {
      return deployApplication(responder, namespace, null, archiveName, configString,
          ownerPrincipal, updateSchedules);
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
          "Deploy failed: " + ex.getMessage());
      return null;
    }
  }

  /**
   * Deploy an application with the specified name and app-id from an artifact.
   */
  @Deprecated
  @POST
  @Path("/apps/{app-id}/versions/{version-id}/create")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public BodyConsumer createAppVersion(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespaceId,
      @PathParam("app-id") final String appId,
      @PathParam("version-id") final String versionId)
      throws Exception {

    // If LCM flow is enabled - Ignore the version provided by the user. Treating it the same as deploy without version
    if (Feature.LIFECYCLE_MANAGEMENT_EDIT.isEnabled(featureFlagsProvider)) {
      return create(request, responder, namespaceId, appId);
    }

    ApplicationId applicationId = validateApplicationVersionId(namespaceId, appId, versionId);

    try {
      return deployAppFromArtifact(applicationId);
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
          "Deploy failed: " + ex.getMessage());
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
      @QueryParam("artifactVersion") String artifactVersion,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize,
      @QueryParam("orderBy") SortOrder orderBy,
      @QueryParam("nameFilter") String nameFilter,
      @QueryParam("nameFilterType") NameFilterType nameFilterType,
      @QueryParam("latestOnly") @DefaultValue("true") Boolean latestOnly,
      @QueryParam("sortCreationTime") Boolean sortCreationTime)
      throws Exception {

    validateNamespace(namespaceId);

    Set<String> names = new HashSet<>();
    if (!Strings.isNullOrEmpty(artifactName)) {
      for (String name : Splitter.on(',').split(artifactName)) {
        names.add(name);
      }
    }

    if (Optional.ofNullable(pageSize).orElse(0) != 0) {
      JsonPaginatedListResponder.respond(GSON, responder, APP_LIST_PAGINATED_KEY,
          jsonListResponder -> {
            AtomicReference<ApplicationRecord> lastRecord = new AtomicReference<>(null);
            ScanApplicationsRequest scanRequest = getScanRequest(namespaceId, artifactVersion,
                pageToken, pageSize,
                orderBy, nameFilter, names, nameFilterType, latestOnly,
                sortCreationTime);
            boolean pageLimitReached = applicationLifecycleService.scanApplications(scanRequest,
                appDetail -> {
                  ApplicationRecord record = new ApplicationRecord(appDetail);
                  jsonListResponder.send(record);
                  lastRecord.set(record);
                });
            ApplicationRecord record = lastRecord.get();
            return !pageLimitReached || record == null ? null :
                record.getName() + EntityId.IDSTRING_PART_SEPARATOR + record.getAppVersion();
          });
    } else {
      ScanApplicationsRequest scanRequest = getScanRequest(namespaceId, artifactVersion, pageToken,
          null,
          orderBy, nameFilter, names, nameFilterType, latestOnly,
          sortCreationTime);
      JsonWholeListResponder.respond(GSON, responder,
          jsonListResponder -> applicationLifecycleService.scanApplications(scanRequest,
              d -> jsonListResponder.send(new ApplicationRecord(d)))
      );
    }
  }

  private ScanApplicationsRequest getScanRequest(String namespaceId, String artifactVersion,
      String pageToken,
      Integer pageSize, SortOrder orderBy, String nameFilter,
      Set<String> names, NameFilterType nameFilterType, Boolean latestOnly,
      Boolean sortCreationTime) {
    ScanApplicationsRequest.Builder builder = ScanApplicationsRequest.builder();
    builder.setNamespaceId(new NamespaceId(namespaceId));
    if (pageSize != null) {
      builder.setLimit(pageSize);
    }
    if (nameFilter != null && !nameFilter.isEmpty()) {
      if (nameFilterType != null) {
        switch (nameFilterType) {
          case EQUALS:
            builder.setApplicationReference(new ApplicationReference(namespaceId, nameFilter));
            break;
          case CONTAINS:
            builder.addFilter(new ApplicationFilter.ApplicationIdContainsFilter(nameFilter));
            break;
          case EQUALS_IGNORE_CASE:
            builder.addFilter(new ApplicationFilter.ApplicationIdEqualsFilter(nameFilter));
        }
      } else {
        // if null, default to use contains
        builder.addFilter(new ApplicationFilter.ApplicationIdContainsFilter(nameFilter));
      }
    }
    builder.addFilters(applicationLifecycleService.getAppFilters(names, artifactVersion));
    if (orderBy != null) {
      builder.setSortOrder(orderBy);
    }
    if (latestOnly != null) {
      builder.setLatestOnly(latestOnly);
    }
    if (sortCreationTime != null) {
      builder.setSortCreationTime(sortCreationTime);
    }
    if (pageToken != null && !pageToken.isEmpty()) {
      builder.setScanFrom(ApplicationId.fromIdParts(Iterables.concat(
          Collections.singleton(namespaceId),
          Arrays.asList(EntityId.IDSTRING_PART_SEPARATOR_PATTERN.split(pageToken))
      )));
    }
    return builder.build();
  }

  /**
   * Returns the info associated with the latest application.
   */
  @GET
  @Path("/apps/{app-id}")
  public void getAppInfo(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespaceId,
      @PathParam("app-id") final String appName) throws Exception {
    // The version of the validated applicationId is ignored. We only use the method to validate the input.
    validateApplicationId(namespaceId, appName);
    responder.sendJson(HttpResponseStatus.OK,
        GSON.toJson(applicationLifecycleService.getLatestAppDetail(
            new ApplicationReference(namespaceId, appName))));
  }

  /**
   * Returns the list of versions of the application.
   */
  @GET
  @Path("/apps/{app-id}/versions")
  public void listAppVersions(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespaceId,
      @PathParam("app-id") final String appName) throws Exception {
    validateApplicationId(namespaceId, appName);
    // enforce GET privileges on the app
    accessEnforcer.enforce(new ApplicationId(namespaceId, appName), authenticationContext.getPrincipal(),
                           StandardPermission.GET);
    Collection<String> versions = applicationLifecycleService
        .getAppVersions(new ApplicationReference(namespaceId, appName));
    if (versions.isEmpty()) {
      throw new ApplicationNotFoundException(new ApplicationId(namespaceId, appName));
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
    ApplicationDetail appDetail = ApplicationId.DEFAULT_VERSION.equals(versionId)
        ? applicationLifecycleService.getLatestAppDetail(applicationId.getAppReference())
        : applicationLifecycleService.getAppDetail(applicationId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(appDetail));
  }

  /**
   * Returns the plugins in the application.
   */
  @GET
  @Path("/apps/{app-id}/plugins")
  public void getPluginsInfo(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespaceId,
      @PathParam("app-id") final String appName) throws Exception {
    // The version of the validated applicationId is ignored. We only use the method to validate the input.
    validateApplicationId(namespaceId, appName);
    responder.sendJson(HttpResponseStatus.OK,
        GSON.toJson(applicationLifecycleService.getPlugins(
            new ApplicationReference(namespaceId, appName))));
  }

  /**
   * Delete an application specified by appId - removes all the versions of the app.
   */
  @DELETE
  @Path("/apps/{app-id}")
  public void deleteApp(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") final String appName) throws Exception {
    validateApplicationId(namespaceId, appName);
    LOG.info("Removing application {} in namespace {}", appName, namespaceId);
    applicationLifecycleService.removeApplication(new ApplicationReference(namespaceId, appName));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Deletes all application state stored in CDAP state store for the given application.
   *
   * @param request The {@link HttpRequest}.
   * @param responder The {@link HttpResponder}.
   * @param namespaceId Namespace id string.
   * @param appName App name string.
   * @throws Exception Any {@link Exception} encountered.
   */
  @DELETE
  @Path("/apps/{app-id}/state")
  public void deleteAppState(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") final String appName) throws Exception {
    LOG.debug("Deleting all application state for {} in namespace {}", appName, namespaceId);
    applicationLifecycleService.deleteAllStates(new NamespaceId(namespaceId), appName);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Delete an application specified by appId and versionId.
   * Deprecated : Version specific deletion is not allowed.
   */
  @Deprecated
  @DELETE
  @Path("/apps/{app-id}/versions/{version-id}")
  public void deleteAppVersion(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespaceId,
      @PathParam("app-id") final String appId,
      @PathParam("version-id") final String versionId) throws Exception {
    // If LCM flow is enabled - we do not want to delete specific versions of the app.
    if (Feature.LIFECYCLE_MANAGEMENT_EDIT.isEnabled(featureFlagsProvider)) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
          "Deletion of specific app version is not allowed.");
      return;
    }
    ApplicationId id = validateApplicationVersionId(namespaceId, appId, versionId);
    applicationLifecycleService.removeApplicationVersion(id);
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

  private ArtifactScope validateScope(String scope) throws BadRequestException {
    try {
      return ArtifactScope.valueOf(scope.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Invalid scope " + scope);
    }
  }

  private Set<ArtifactScope> getArtifactScopes(Set<String> artifactScopes)
      throws BadRequestException {
    // If no scope is provided, consider all artifact scopes.
    if (artifactScopes.isEmpty()) {
      return EnumSet.allOf(ArtifactScope.class);
    }
    Set<ArtifactScope> scopes = new HashSet<>();
    for (String scope : artifactScopes) {
      scopes.add(validateScope(scope));
    }
    return scopes;
  }

  /**
   * Updates an existing application.
   * Deprecated : Unused - just another deploy action after introduction of edit versions
   */
  @Deprecated
  @POST
  @Path("/apps/{app-id}/update")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateApp(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespaceId,
      @PathParam("app-id") final String appName)
      throws NotFoundException, BadRequestException, AccessException, IOException {

    validateApplicationId(namespaceId, appName);

    AppRequest appRequest;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()),
        StandardCharsets.UTF_8)) {
      appRequest = DECODE_GSON.fromJson(reader, AppRequest.class);
    } catch (IOException e) {
      LOG.error("Error reading request to update app {} in namespace {}.", appName, namespaceId, e);
      throw new IOException("Error reading request body.");
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }

    try {
      applicationLifecycleService.updateApp(new ApplicationId(namespaceId, appName), appRequest,
          createProgramTerminator());
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
   * upgrades the existing application.
   */
  @POST
  @Path("/apps/{app-id}/upgrade")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void upgradeApplication(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("app-id") String appName,
      @QueryParam("artifactScope") Set<String> artifactScopes,
      @QueryParam("allowSnapshot") boolean allowSnapshot) throws Exception {
    validateApplicationId(namespaceId, appName);
    Set<ArtifactScope> allowedArtifactScopes = getArtifactScopes(artifactScopes);
    // Always upgrade the latest version
    ApplicationId appId = applicationLifecycleService.upgradeLatestApplication(
        new ApplicationReference(namespaceId, appName), allowedArtifactScopes, allowSnapshot);
    ApplicationUpdateDetail updateDetail = new ApplicationUpdateDetail(appId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(updateDetail));
  }

  /**
   * Upgrades a lis of existing application to use latest version of application artifact and plugin
   * artifacts.
   *
   * <pre>
   * {@code
   * [
   *   {"name":"XYZ"},
   *   {"name":"ABC"},
   *   {"name":"FOO"},
   * ]
   * }
   * </pre>
   * The response will be an array of {@link ApplicationUpdateDetail} object, which either indicates
   * a success (200) or failure for each of the requested application in the same order as the
   * request. The failure also indicates reason for the error. The response will be sent via
   * ChunkResponder to continuously stream upgrade result per application.
   */
  @POST
  @Path("/upgrade")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void upgradeApplications(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @QueryParam("artifactScope") Set<String> artifactScopes,
      @QueryParam("allowSnapshot") boolean allowSnapshot) throws Exception {
    // TODO: (CDAP-16910) Improve batch API performance as each application upgrade is an event independent of each
    //  other.

    List<ApplicationId> appIds = decodeAndValidateBatchApplicationRecord(
        validateNamespace(namespaceId), request);
    Set<ArtifactScope> allowedArtifactScopes = getArtifactScopes(artifactScopes);
    try (ChunkResponder chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK)) {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      try (JsonWriter jsonWriter = new JsonWriter(
          new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
        jsonWriter.beginArray();
        for (ApplicationId appId : appIds) {
          ApplicationUpdateDetail updateDetail;
          try {
            ApplicationId newAppId = ApplicationId.DEFAULT_VERSION.equals(appId.getVersion())
                ? applicationLifecycleService.upgradeLatestApplication(appId.getAppReference(),
                allowedArtifactScopes, allowSnapshot)
                : applicationLifecycleService.upgradeApplication(appId, allowedArtifactScopes,
                    allowSnapshot);
            updateDetail = new ApplicationUpdateDetail(newAppId);
          } catch (UnsupportedOperationException e) {
            String errorMessage = String.format("Application %s does not support upgrade.", appId);
            updateDetail = new ApplicationUpdateDetail(appId,
                new NotImplementedException(errorMessage));
          } catch (InvalidArtifactException | NotFoundException e) {
            updateDetail = new ApplicationUpdateDetail(appId, e);
          } catch (Exception e) {
            updateDetail = new ApplicationUpdateDetail(appId,
                new ServiceException("Upgrade failed due to internal error.", e,
                    HttpResponseStatus.INTERNAL_SERVER_ERROR));
            LOG.error("Application upgrade failed with exception", e);
          }
          GSON.toJson(updateDetail, ApplicationUpdateDetail.class, jsonWriter);
          jsonWriter.flush();
          chunkResponder.sendChunk(Unpooled.wrappedBuffer(outputStream.toByteArray()));
          outputStream.reset();
          chunkResponder.flush();
        }
        jsonWriter.endArray();
      }
      chunkResponder.sendChunk(Unpooled.wrappedBuffer(outputStream.toByteArray()));
    }
  }

  /**
   * Gets {@link ApplicationDetail} for a set of applications. It expects a post body as a array of
   * object, with each object specifying the applciation id and an optional version. E.g.
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
   * The response will be an array of {@link BatchApplicationDetail} object, which either indicates
   * a success (200) or failure for each of the requested application in the same order as the
   * request.
   *
   */
  @POST
  @Path("/appdetail")
  public void getApplicationDetails(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace) throws Exception {

    List<ApplicationId> appIds = decodeAndValidateBatchApplication(validateNamespace(namespace),
        request);
    Map<ApplicationId, ApplicationDetail> details = applicationLifecycleService.getAppDetails(
        appIds);

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
   * Decodes request coming from the {@link #getApplicationDetails(FullHttpRequest, HttpResponder,
   * String)} call.
   */
  private List<ApplicationId> decodeAndValidateBatchApplication(NamespaceId namespaceId,
      FullHttpRequest request) throws BadRequestException {
    try {
      List<ApplicationId> result = new ArrayList<>();
      JsonArray array = DECODE_GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
          JsonArray.class);
      if (array == null) {
        throw new BadRequestException(
            "Request body is invalid json, please check that it is a json array.");
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
        String version = Optional.ofNullable(obj.get("version"))
            .map(JsonElement::getAsString)
            .orElse(ApplicationId.DEFAULT_VERSION);
        result.add(validateApplicationVersionId(namespaceId, appId, version));
      }
      return result;
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }
  }

  /**
   * Decodes request coming from the {@link #upgradeApplications(FullHttpRequest, HttpResponder,
   * String, Set, boolean)} call.
   */
  private List<ApplicationId> decodeAndValidateBatchApplicationRecord(NamespaceId namespaceId,
      FullHttpRequest request)
      throws BadRequestException {
    try {
      List<ApplicationId> appIds = new ArrayList<>();
      List<ApplicationRecord> records =
          DECODE_GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
              new TypeToken<List<ApplicationRecord>>() {
              }.getType());
      if (records == null) {
        throw new BadRequestException(
            "Request body is invalid json, please check that it is a json array.");
      }
      for (ApplicationRecord element : records) {
        if (element.getName() != null && element.getName().isEmpty()) {
          throw new BadRequestException("Missing 'name' in the request element for app-id.");
        }
        if (element.getAppVersion() == null) {
          validateApplicationId(namespaceId, element.getName());
          appIds.add(namespaceId.app(element.getName()));
        } else {
          appIds.add(validateApplicationVersionId(namespaceId, element.getName(),
              element.getAppVersion()));
        }
      }
      return appIds;
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }
  }

  // normally we wouldn't want to use a body consumer but would just want to read the request body directly
  // since it wont be big. But the deploy app API has one path with different behavior based on content type
  // the other behavior requires a BodyConsumer and only have one method per path is allowed,
  // so we have to use a BodyConsumer
  private BodyConsumer deployAppFromArtifact(final ApplicationId appId) throws IOException {
    // Perform auth checks outside BodyConsumer as only the first http request containing auth header
    // to populate SecurityRequestContext while http chunk doesn't. BodyConsumer runs in the thread
    // that processes the last http chunk.
    accessEnforcer.enforce(appId, authenticationContext.getPrincipal(), StandardPermission.CREATE);
    LOG.info("Start to deploy app {} in namespace {} by user {}", appId.getApplication(),
        appId.getParent(),
        applicationLifecycleService.decodeUserId(authenticationContext));
    // createTempFile() needs a prefix of at least 3 characters
    return deployAppFromArtifact(appId, false);
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

    KerberosPrincipalId ownerPrincipalId =
        ownerPrincipal == null ? null : new KerberosPrincipalId(ownerPrincipal);

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
              applicationLifecycleService.deployAppAndArtifact(namespace, appId, artifactId,
                  uploadedFile, configString,
                  finalOwnerPrincipalId, createProgramTerminator(),
                  updateSchedules);
          LOG.info(
              "Successfully deployed app {} in namespace {} from artifact {} with configuration {} and "

                  + "principal {}", app.getApplicationId().getApplication(),
              namespace.getNamespace(),
              artifactId,
              configString, finalOwnerPrincipalId);
          responder.sendJson(HttpResponseStatus.OK, GSON.toJson(getApplicationRecord(app)));
        } catch (InvalidArtifactException e) {
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        } catch (ArtifactAlreadyExistsException e) {
          responder.sendString(HttpResponseStatus.CONFLICT, String.format(
              "Artifact '%s' already exists. Please use the API that creates an application from an existing artifact. "

                  + "If you are trying to replace the artifact, please delete it and then try again.",
              artifactId));
        } catch (WriteConflictException e) {
          // don't really expect this to happen. It means after multiple retries there were still write conflicts.
          LOG.warn("Write conflict while trying to add artifact {}.", artifactId, e);
          responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
              "Write conflict while adding artifact. This can happen if multiple requests to add "
                  + "the same artifact occur simultaneously. Please try again.");
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

  private void validateApplicationId(@Nullable String namespace, @Nullable String appId)
      throws BadRequestException, NamespaceNotFoundException, AccessException {
    validateApplicationId(validateNamespace(namespace), appId);
  }

  private void validateApplicationId(NamespaceId namespaceId, String appId)
      throws BadRequestException {
    validateApplicationVersionId(namespaceId, appId, ApplicationId.DEFAULT_VERSION);
  }

  private ApplicationId validateApplicationVersionId(@Nullable String namespace,
      @Nullable String appId,
      @Nullable String versionId)
      throws BadRequestException, NamespaceNotFoundException, AccessException {
    return validateApplicationVersionId(validateNamespace(namespace), appId, versionId);
  }



  @POST
  @Path("/apps/{app-id}/summarize")
  public void summarizeApplication(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespaceId,
      @PathParam("app-id") final String appId) throws Exception {

    // Fetch pipeline JSON from apps table
//    getPipelineJsonFromAppsTable pipelinejsontableclass= new getPipelineJsonFromAppsTable();
//    String pipelineJson=pipelinejsontableclass.getPipelineJsonFromAppsTablefunction(namespaceId,appId);

    ApplicationDetail applicationDetail = applicationLifecycleService.getLatestAppDetail(
        new ApplicationReference(namespaceId, appId));

    String pipelineJson = GSON.toJson(applicationDetail);

    // Summarize pipeline using GenAI
    generateSummaryFromPipelineJson pipelinesummary=new generateSummaryFromPipelineJson();
    String summary = pipelinesummary.generateSummaryFromPipelineJsonfunction(pipelineJson);

    // Send the summary response
    // JsonObject summaryJson = new JsonObject();
    // summaryJson.addProperty("summary", summary);
    responder.sendJson(HttpResponseStatus.OK, summary);
  }

  @POST
  @Path("/system/apps/pipeline/services/studio/methods/v1/contexts/{namespace_id}/drafts/{draft_id}/summarize")
  public void summarizeApplicationdraft(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") final String namespaceId,
                                   @PathParam("app-id") final String appId) throws Exception {


    String pipelineJson=request.toString();
    //JsonParser parser= new JsonParser();
   // JsonObject reqob= parser.parse(reqbody).getAsJsonObject();
    //String pipelineJson= reqob.get(pipeline);
    // Summarize pipeline using GenAI
    generateSummaryFromPipelineJson pipelinesummary=new generateSummaryFromPipelineJson();
    String summary = pipelinesummary.generateSummaryFromPipelineJsonfunction(pipelineJson);
    // Send the summary response
    // JsonObject summaryJson = new JsonObject();
    // summaryJson.addProperty("summary", summary);
    responder.sendJson(HttpResponseStatus.OK, summary);
  }

  @GET
  @Path("/apps/hello")
  public void hell(HttpRequest request, HttpResponder responder) throws Exception {
    try {
      String s = "+\n" +
              "\"drive.web-frontend_20240707.13_p0\n\" +\n" +
              "\"{\n\" +\n" +
              "\"    \"name\": \"PORTLAND_SE_DEV_GIT\",\n\" +\n" +
              "\"    \"description\": \"Data Pipeline Application\",\n\" +\n" +
              "\"    \"artifact\": {\n\" +\n" +
              "\"        \"name\": \"cdap-data-pipeline\",\n\" +\n" +
              "\"        \"version\": \"[6.1.1,7.0.0)\",\n\" +\n" +
              "\"        \"scope\": \"SYSTEM\"\n\" +\n" +
              "\"    },\n\" +\n" +
              "\"    \"config\": {\n\" +\n" +
              "\"        \"resources\": {\n\" +\n" +
              "\"            \"memoryMB\": 4096,\n\" +\n" +
              "\"            \"virtualCores\": 2\n\" +\n" +
              "\"        },\n\" +\n" +
              "\"        \"driverResources\": {\n\" +\n" +
              "\"            \"memoryMB\": 4096,\n\" +\n" +
              "\"            \"virtualCores\": 2\n\" +\n" +
              "\"        },\n\" +\n" +
              "\"        \"connections\": [\n\" +\n" +
              "\"            {\n\" +\n" +
              "\"                \"from\": \"portland_raw\",\n\" +\n" +
              "\"                \"to\": \"clean_format_raw\"\n\" +\n" +
              "\"            },\n\" +\n" +
              "\"            {\n\" +\n" +
              "\"                \"from\": \"clean_format_raw\",\n\" +\n" +
              "\"                \"to\": \"remove \\\"NULL\\\" strings + lowercase with JS\"\n\" +\n" +
              "\"            },\n\" +\n" +
              "\"            {\n\" +\n" +
              "\"                \"from\": \"remove \\\"NULL\\\" strings + lowercase with JS\",\n\" +\n" +
              "\"                \"to\": \"se_account_spec_out\"\n\" +\n" +
              "\"            }\n\" +\n" +
              "\"        ],\n\" +\n" +
              "\"        \"comments\": [],\n\" +\n" +
              "\"        \"postActions\": [],\n\" +\n" +
              "\"        \"properties\": {},\n\" +\n" +
              "\"        \"processTimingEnabled\": true,\n\" +\n" +
              "\"        \"stageLoggingEnabled\": false,\n\" +\n" +
              "\"        \"stages\": [\n\" +\n" +
              "\"            {\n\" +\n" +
              "\"                \"name\": \"portland_raw\",\n\" +\n" +
              "\"                \"plugin\": {\n\" +\n" +
              "\"                    \"name\": \"GCSFile\",\n\" +\n" +
              "\"                    \"type\": \"batchsource\",\n\" +\n" +
              "\"                    \"label\": \"portland_raw\",\n\" +\n" +
              "\"                    \"artifact\": {\n\" +\n" +
              "\"                        \"name\": \"google-cloud\",\n\" +\n" +
              "\"                        \"version\": \"[0.13.2,1.0.0)\",\n\" +\n" +
              "\"                        \"scope\": \"SYSTEM\"\n\" +\n" +
              "\"                    },\n\" +\n" +
              "\"                    \"properties\": {\n\" +\n" +
              "\"                        \"project\": \"${project_id}\",\n\" +\n" +
              "\"                        \"format\": \"text\",\n\" +\n" +
              "\"                        \"skipHeader\": \"false\",\n\" +\n" +
              "\"                        \"serviceFilePath\": \"auto-detect\",\n\" +\n" +
              "\"                        \"filenameOnly\": \"false\",\n\" +\n" +
              "\"                        \"recursive\": \"false\",\n\" +\n" +
              "\"                        \"encrypted\": \"false\",\n\" +\n" +
              "\"                        \"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"body\\\",\\\"type\\\":\\\"string\\\"}]}\",\n\" +\n" +
              "\"                        \"referenceName\": \"portland\",\n\" +\n" +
              "\"                        \"path\": \"gs://${source_bucket_name}/${source_folder_path}/${raw_file_name_pattern}_*\",\n\" +\n" +
              "\"                        \"delimiter\": \",\"\n\" +\n" +
              "\"                    }\n\" +\n" +
              "\"                },\n\" +\n" +
              "\"                \"outputSchema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"body\\\",\\\"type\\\":\\\"string\\\"}]}\",\n\" +\n" +
              "\"                \"id\": \"portland_raw\",\n\" +\n" +
              "\"                \"type\": \"batchsource\",\n\" +\n" +
              "\"                \"label\": \"portland_raw\",\n\" +\n" +
              "\"                \"icon\": \"fa-plug\"\n\" +\n" +
              "\"            },\n\" +\n" +
              "\"            {\n\" +\n" +
              "\"                \"name\": \"clean_format_raw\",\n\" +\n" +
              "\"                \"plugin\": {\n\" +\n" +
              "\"                    \"name\": \"Wrangler\",\n\" +\n" +
              "\"                    \"type\": \"transform\",\n\" +\n" +
              "\"                    \"label\": \"clean_format_raw\",\n\" +\n" +
              "\"                    \"artifact\": {\n\" +\n" +
              "\"                        \"name\": \"wrangler-transform\",\n\" +\n" +
              "\"                        \"version\": \"[4.1.4,5.0.0)\",\n\" +\n" +
              "\"                        \"scope\": \"SYSTEM\"\n\" +\n" +
              "\"                    },\n\" +\n" +
              "\"                    \"properties\": {\n\" +\n" +
              "\"                        \"field\": \"*\",\n\" +\n" +
              "\"                        \"precondition\": \"false\",\n\" +\n" +
              "\"                        \"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"account_number\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_opened_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_closed_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_account_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_secondary_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_premise_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type_units\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"first_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"last_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_customer_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_phone\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_email\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_email_opt_out\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_is_owner\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_year_built\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rate_codes\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_heat_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rebates_used\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_enrolled_programs\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_device_types\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_meter_quality\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_test_bed_region\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_latitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_longitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_has_pool\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_low_income_assistance_electric\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_sq_ft\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_status\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\",\n\" +\n" +
              "\"                        \"workspaceId\": \"6515dfe5-8e3d-4095-a4d5-84b1fe226aa5\",\n\" +\n" +
              "\"                        \"directives\": \"parse-as-csv :body ',' true\\ndrop body\\nrename ACCOUNT_ID account_number\\nrename ACTIVE_DATE fact_opened_at\\nrename INACTIVE_DATE fact_closed_at\\nrename ADDRESS address1\\nset-column address2 ''\\nrename SERVICE_CITY city\\nrename SERVICE_STATE state\\nset-column zip (string:substring(SERVICE_ZIP_CODE, 0, 5))\\nset-column fact_account_type (IS_RESIDENTIAL == 'Y'  ? 'residential' : 'commercial')\\nrename SECONDARY_ID fact_secondary_id\\nset-column fact_commodity_type (METER_FUEL_TYPE == 'E'  ? 'electric' : METER_FUEL_TYPE)\\nrename PREMISE_ID fact_premise_id\\nrename METER_UNITS fact_commodity_type_units\\nrename CUSTOMER_ID fact_customer_id\\nrename PHONE_1 account_phone\\nrename EMAIL account_email\\nrename EMAIL_OP_OUT_STATUS fact_email_opt_out\\nrename OWNER fact_is_owner\\nrename YEAR_BUILT fact_year_built\\nrename RATE_CODE multifact_rate_codes\\nrename HEAT_TYPE fact_heat_type\\nrename REBATES_USED multifact_rebates_used\\nrename ENROLLED_PROGRAMS multifact_enrolled_programs\\nrename PREMISE_DEVICES multifact_device_types\\nrename METER_QUALITY fact_meter_quality\\nrename TEST_BED_REGION fact_test_bed_region\\nrename MAIL_ADDRESS_LINE1 fact_mail_address_line_1\\nrename MAIL_ADDRESS_LINE2 fact_mail_address_line_2\\nrename MAIL_CITY fact_mail_city\\nrename MAIL_STATE fact_mail_state\\nrename MAIL_ZIP_CODE fact_mail_zip\\nrename LATITUDE fact_latitude\\nrename LONGITUDE fact_longitude\\nrename POOL fact_has_pool\\nrename LOW_INCOME_ASSIST_INCOME_QUAL fact_low_income_assistance_electric\\nrename SQ_FT fact_sq_ft\\nset-column account_status 'opened'\\nfind-and-replace multifact_enrolled_programs s/;/,/g\\nfind-and-replace multifact_rate_codes s/;/,/g\\nfind-and-replace multifact_rebates_used s/;/,/g\\nfind-and-replace multifact_device_types s/;/,/g\\nfilter-rows-on regex-match account_email ^(?i)email$\\nfilter-rows-on regex-match account_number ^(?i)ACCOUNT_ID$\\nfilter-rows-on regex-match fact_opened_at ^(?i)ACTIVE_DATE$\\nfilter-rows-on regex-match fact_closed_at ^(?i)INACTIVE_DATE$\\nfilter-rows-on regex-match city ^(?i)SERVICE_CITY$\\nfilter-rows-on regex-match state ^(?i)SERVICE_STATE$\\nfilter-rows-on regex-match fact_secondary_id ^(?i)SECONDARY_ID$\\nfilter-rows-on regex-match fact_premise_id ^(?i)PREMISE_ID$\\nfilter-rows-on regex-match fact_commodity_type_units ^(?i)METER_UNITS$\\nfilter-rows-on regex-match LAST_NAME ^(?i)LAST_NAME$\\nfilter-rows-on regex-match FIRST_NAME ^(?i)FIRST_NAME$\\nfilter-rows-on regex-match fact_customer_id ^(?i)CUSTOMER_ID$\\nfilter-rows-on regex-match account_phone ^(?i)PHONE_1$\\nfilter-rows-on regex-match account_email ^(?i)EMAIL$\\nfilter-rows-on regex-match fact_is_owner ^(?i)OWNER$\\nfilter-rows-on regex-match multifact_rate_codes ^(?i)\\\"RATE_CODE\\\"$\\nfilter-rows-on regex-match address1 ^(?i)ADDRESS$\\nfilter-rows-on regex-match SERVICE_ZIP_CODE ^(?i)SERVICE_ZIP_CODE$\\nfilter-rows-on regex-match zip ^(?i)SERVICE_ZIP_CODE$\"\n\" +\n" +
              "\"                    }\n\" +\n" +
              "\"                },\n\" +\n" +
              "\"                \"outputSchema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"account_number\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_opened_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_closed_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_account_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_secondary_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_premise_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type_units\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"first_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"last_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_customer_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_phone\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_email\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_email_opt_out\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_is_owner\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_year_built\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rate_codes\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_heat_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rebates_used\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_enrolled_programs\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_device_types\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_meter_quality\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_test_bed_region\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_latitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_longitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_has_pool\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_low_income_assistance_electric\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_sq_ft\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_status\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\",\n\" +\n" +
              "\"                \"inputSchema\": [\n\" +\n" +
              "\"                    {\n\" +\n" +
              "\"                        \"name\": \"portland_raw\",\n\" +\n" +
              "\"                        \"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"body\\\",\\\"type\\\":\\\"string\\\"}]}\"\n\" +\n" +
              "\"                    }\n\" +\n" +
              "\"                ],\n\" +\n" +
              "\"                \"id\": \"clean_format_raw\",\n\" +\n" +
              "\"                \"type\": \"transform\",\n\" +\n" +
              "\"                \"label\": \"clean_format_raw\",\n\" +\n" +
              "\"                \"icon\": \"icon-DataPreparation\"\n\" +\n" +
              "\"            },\n\" +\n" +
              "\"            {\n\" +\n" +
              "\"                \"name\": \"remove \\\"NULL\\\" strings + lowercase with JS\",\n\" +\n" +
              "\"                \"plugin\": {\n\" +\n" +
              "\"                    \"name\": \"JavaScript\",\n\" +\n" +
              "\"                    \"type\": \"transform\",\n\" +\n" +
              "\"                    \"label\": \"remove \\\"NULL\\\" strings + lowercase with JS\",\n\" +\n" +
              "\"                    \"artifact\": {\n\" +\n" +
              "\"                        \"name\": \"core-plugins\",\n\" +\n" +
              "\"                        \"version\": \"[2.3.4,3.0.0)\",\n\" +\n" +
              "\"                        \"scope\": \"SYSTEM\"\n\" +\n" +
              "\"                    },\n\" +\n" +
              "\"                    \"properties\": {\n\" +\n" +
              "\"                        \"script\": \"/**\\n * @summary Transforms the provided input record into zero or more output records or errors.\\n\\n * Input records are available in JavaScript code as JSON objects. \\n\\n * @param input an object that contains the input record as a JSON.   e.g. to access a field called 'total' from the input record, use input.total.\\n * @param emitter an object that can be used to emit zero or more records (using the emitter.emit() method) or errors (using the emitter.emitError() method) \\n * @param context an object that provides access to:\\n *            1. CDAP Metrics - context.getMetrics().count('output', 1);\\n *            2. CDAP Logs - context.getLogger().debug('Received a record');\\n *            3. Lookups - context.getLookup('blacklist').lookup(input.id); or\\n *            4. Runtime Arguments - context.getArguments().get('priceThreshold') \\n */ \\n\\nfunction yyyymmdd(datestring) {\\n  var cleaned = datestring.replace(/-/g, \\\"\\\").split(\\\" \\\")[0]\\n  return cleaned;\\n}\\n\\nvar dateColumns = ['fact_closed_at', 'fact_opened_at']\\n\\nfunction transform(input, emitter, context) {\\n  var clearNullValuesAndLower = Object.keys(input).reduce(function (acc, k) {\\n    var v = input[k] === \\\"NULL\\\" ? \\\"\\\" : input[k].toLowerCase();\\n    // format date columns\\n    if ((v !== \\\"\\\") && dateColumns.indexOf(k) != -1) {\\n      v = yyyymmdd(v)\\n    }\\n    acc[k] = '\\\"'+v+'\\\"';\\n    return acc;\\n  }, {});\\n  emitter.emit(clearNullValuesAndLower);\\n}\",\n\" +\n" +
              "\"                        \"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"account_number\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_opened_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_closed_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_account_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_secondary_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_premise_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type_units\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"first_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"last_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_customer_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_phone\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_email\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_email_opt_out\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_is_owner\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_year_built\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rate_codes\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_heat_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rebates_used\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_enrolled_programs\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_device_types\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_meter_quality\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_test_bed_region\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_latitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_longitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_has_pool\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_low_income_assistance_electric\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_sq_ft\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_status\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\"\n\" +\n" +
              "\"                    }\n\" +\n" +
              "\"                },\n\" +\n" +
              "\"                \"outputSchema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"account_number\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_opened_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_closed_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_account_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_secondary_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_premise_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type_units\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"first_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"last_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_customer_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_phone\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_email\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_email_opt_out\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_is_owner\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_year_built\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rate_codes\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_heat_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rebates_used\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_enrolled_programs\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_device_types\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_meter_quality\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_test_bed_region\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_latitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_longitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_has_pool\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_low_income_assistance_electric\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_sq_ft\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_status\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\",\n\" +\n" +
              "\"                \"inputSchema\": [\n\" +\n" +
              "\"                    {\n\" +\n" +
              "\"                        \"name\": \"clean_format_raw\",\n\" +\n" +
              "\"                        \"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"account_number\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_opened_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_closed_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_account_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_secondary_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_premise_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type_units\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"first_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"last_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_customer_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_phone\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_email\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_email_opt_out\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_is_owner\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_year_built\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rate_codes\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_heat_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rebates_used\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_enrolled_programs\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_device_types\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_meter_quality\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_test_bed_region\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_latitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_longitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_has_pool\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_low_income_assistance_electric\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_sq_ft\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_status\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\"\n\" +\n" +
              "\"                    }\n\" +\n" +
              "\"                ],\n\" +\n" +
              "\"                \"id\": \"remove-\\\"NULL\\\"-strings-+-lowercase-with-JS\",\n\" +\n" +
              "\"                \"type\": \"transform\",\n\" +\n" +
              "\"                \"label\": \"remove \\\"NULL\\\" strings + lowercase with JS\",\n\" +\n" +
              "\"                \"icon\": \"icon-javascript\"\n\" +\n" +
              "\"            },\n\" +\n" +
              "\"            {\n\" +\n" +
              "\"                \"name\": \"se_account_spec_out\",\n\" +\n" +
              "\"                \"plugin\": {\n\" +\n" +
              "\"                    \"name\": \"GCS\",\n\" +\n" +
              "\"                    \"type\": \"batchsink\",\n\" +\n" +
              "\"                    \"label\": \"se_account_spec_out\",\n\" +\n" +
              "\"                    \"artifact\": {\n\" +\n" +
              "\"                        \"name\": \"google-cloud\",\n\" +\n" +
              "\"                        \"version\": \"[0.13.2,1.0.0)\",\n\" +\n" +
              "\"                        \"scope\": \"SYSTEM\"\n\" +\n" +
              "\"                    },\n\" +\n" +
              "\"                    \"properties\": {\n\" +\n" +
              "\"                        \"project\": \"auto-detect\",\n\" +\n" +
              "\"                        \"suffix\": \"${logicalStartTime(yyyy-MM-dd-HH-mm-ss)}\",\n\" +\n" +
              "\"                        \"format\": \"csv\",\n\" +\n" +
              "\"                        \"serviceFilePath\": \"auto-detect\",\n\" +\n" +
              "\"                        \"location\": \"us\",\n\" +\n" +
              "\"                        \"path\": \"gs://${destination_bucket_name}/${destination_folder_path}/${cdf_instance_name}/\",\n\" +\n" +
              "\"                        \"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"account_number\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_opened_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_closed_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_account_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_secondary_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_premise_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type_units\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"first_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"last_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_customer_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_phone\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_email\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_email_opt_out\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_is_owner\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_year_built\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rate_codes\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_heat_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rebates_used\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_enrolled_programs\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_device_types\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_meter_quality\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_test_bed_region\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_latitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_longitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_has_pool\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_low_income_assistance_electric\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_sq_ft\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_status\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\",\n\" +\n" +
              "\"                        \"referenceName\": \"se_account_spec\",\n\" +\n" +
              "\"                        \"writeHeader\": \"false\",\n\" +\n" +
              "\"                        \"serviceAccountType\": \"filePath\",\n\" +\n" +
              "\"                        \"contentType\": \"application/octet-stream\"\n\" +\n" +
              "\"                    }\n\" +\n" +
              "\"                },\n\" +\n" +
              "\"                \"outputSchema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"account_number\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_opened_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_closed_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_account_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_secondary_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_premise_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type_units\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"first_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"last_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_customer_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_phone\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_email\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_email_opt_out\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_is_owner\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_year_built\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rate_codes\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_heat_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rebates_used\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_enrolled_programs\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_device_types\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_meter_quality\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_test_bed_region\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_latitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_longitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_has_pool\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_low_income_assistance_electric\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_sq_ft\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_status\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\",\n\" +\n" +
              "\"                \"inputSchema\": [\n\" +\n" +
              "\"                    {\n\" +\n" +
              "\"                        \"name\": \"remove \\\"NULL\\\" strings + lowercase with JS\",\n\" +\n" +
              "\"                        \"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"etlSchemaBody\\\",\\\"fields\\\":[{\\\"name\\\":\\\"account_number\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_opened_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_closed_at\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"address2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_account_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_secondary_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_premise_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_commodity_type_units\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"first_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"last_name\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_customer_id\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_phone\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_email\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_email_opt_out\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_is_owner\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_year_built\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rate_codes\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_heat_type\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_rebates_used\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_enrolled_programs\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"multifact_device_types\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_meter_quality\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_test_bed_region\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_1\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_address_line_2\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_city\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_state\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_mail_zip\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_latitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_longitude\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_has_pool\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_low_income_assistance_electric\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"fact_sq_ft\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"account_status\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}\"\n\" +\n" +
              "\"                    }\n\" +\n" +
              "\"                ],\n\" +\n" +
              "\"                \"id\": \"se_account_spec_out\",\n\" +\n" +
              "\"                \"type\": \"batchsink\",\n\" +\n" +
              "\"                \"label\": \"se_account_spec_out\",\n\" +\n" +
              "\"                \"icon\": \"fa-plug\"\n\" +\n" +
              "\"            }\n\" +\n" +
              "\"        ],\n\" +\n" +
              "\"        \"schedule\": \"0 * * * *\",\n\" +\n" +
              "\"        \"engine\": \"spark\",\n\" +\n" +
              "\"        \"numOfRecordsPreview\": 100,\n\" +\n" +
              "\"        \"description\": \"Data Pipeline Application\",\n\" +\n" +
              "\"        \"maxConcurrentRuns\": 1\n\" +\n" +
              "\"    }\n\" +\n" +
              "\"}\n";
      generateSummaryFromPipelineJson pipelinesummary = new generateSummaryFromPipelineJson();
      String summary = pipelinesummary.generateSummaryFromPipelineJsonfunction(s);
      responder.sendJson(HttpResponseStatus.OK, "hello");
    } catch (Exception e) {
      LOG.error("Failed to say hello", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.toString());
    }
  }

  @GET
  @Path("/apps/ram")
  public void helloram(HttpRequest request, HttpResponder responder) throws Exception {

    responder.sendString(HttpResponseStatus.BAD_REQUEST,"ram");
  }
}

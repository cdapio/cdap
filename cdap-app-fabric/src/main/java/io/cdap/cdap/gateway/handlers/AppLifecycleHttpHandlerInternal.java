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

import com.google.common.collect.Iterables;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.store.ApplicationFilter;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.app.MarkLatestAppsRequest;
import io.cdap.cdap.proto.app.UpdateMultiSourceControlMetaReqeust;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;


/**
 * Internal {@link HttpHandler} for Application Lifecycle Management.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppLifecycleHttpHandlerInternal extends AbstractAppLifecycleHttpHandler {

  private static final String APP_LIST_PAGINATED_KEY = "applications";

  private final FeatureFlagsProvider featureFlagsProvider;

  @Inject
  AppLifecycleHttpHandlerInternal(
      CConfiguration configuration,
      NamespaceQueryAdmin namespaceQueryAdmin,
      ProgramRuntimeService runtimeService,
      ApplicationLifecycleService applicationLifecycleService) {
    super(configuration, namespaceQueryAdmin, runtimeService, applicationLifecycleService);
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(configuration);
  }

  /**
   * Get a list of {@link ApplicationDetail} for all applications in the given namespace.
   *
   * @param request {@link HttpRequest}
   * @param responder {@link HttpResponse}
   * @param namespace the namespace to get all application details
   * @param pageToken the token identifier for the current page requested in a paginated
   *     request
   * @param pageSize the number of application details returned in a paginated request
   * @param orderBy the sorting order in which results are returned, ASC for ascending, DESC for
   *     descending
   * @param nameFilter the filters that must be satisfied  by ApplicationDetail in order to be
   *     returned
   * @throws Exception if namespace doesn't exists or failed to get all application details
   *         TODO: CDAP-18224 - the below code is common with AppLifeCycleHttpHandler
   *         TODO: both these classes will be refactored in a separate PR
   */
  @GET
  @Path("/apps")
  public void getAllAppDetails(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize,
      @QueryParam("orderBy") SortOrder orderBy,
      @QueryParam("nameFilter") String nameFilter) throws Exception {

    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }

    if (Optional.ofNullable(pageSize).orElse(0) != 0) {
      JsonPaginatedListResponder.respond(GSON, responder, APP_LIST_PAGINATED_KEY,
          jsonListResponder -> {
            AtomicReference<ApplicationRecord> lastRecord = new AtomicReference<>(null);
            ScanApplicationsRequest scanRequest = getScanRequest(namespace, pageToken, pageSize,
                orderBy, nameFilter);
            boolean pageLimitReached = applicationLifecycleService.scanApplications(scanRequest,
                appDetail -> {
                  ApplicationRecord record = new ApplicationRecord(appDetail);
                  jsonListResponder.send(appDetail);
                  lastRecord.set(record);
                });
            ApplicationRecord record = lastRecord.get();
            return !pageLimitReached || record == null ? null :
                record.getName() + EntityId.IDSTRING_PART_SEPARATOR + record.getAppVersion();
          });
    } else {
      ScanApplicationsRequest scanRequest = getScanRequest(namespace, pageToken, null,
          orderBy, nameFilter);
      JsonWholeListResponder.respond(GSON, responder,
          jsonListResponder -> applicationLifecycleService.scanApplications(scanRequest,
              jsonListResponder::send)
      );
    }
  }

  private ScanApplicationsRequest getScanRequest(String namespaceId, String pageToken,
      Integer pageSize, SortOrder orderBy, String nameFilter) {
    ScanApplicationsRequest.Builder builder = ScanApplicationsRequest.builder();
    builder.setNamespaceId(new NamespaceId(namespaceId));
    if (pageSize != null) {
      builder.setLimit(pageSize);
    }
    if (nameFilter != null && !nameFilter.isEmpty()) {
      builder.addFilter(new ApplicationFilter.ApplicationIdContainsFilter(nameFilter));
    }
    if (orderBy != null) {
      builder.setSortOrder(orderBy);
    }
    if (pageToken != null && !pageToken.isEmpty()) {
      builder.setScanFrom(ApplicationId.fromIdParts(Iterables.concat(
          Collections.singleton(namespaceId),
          Arrays.asList(EntityId.IDSTRING_PART_SEPARATOR_PATTERN.split(pageToken))
      )));
    }
    // Scan the latest applications only for internal apps list api.
    builder.setLatestOnly(true);
    return builder.build();
  }

  /**
   * Get {@link ApplicationDetail} for a given application.
   *
   * @param request {@link HttpRequest}
   * @param responder {@link HttpResponse}
   * @param namespace the namespace to get all application details   *
   * @param application the id of the application to get its {@link ApplicationDetail}
   * @throws Exception if either namespace or application doesn't exist, or failed to get {@link
   *     ApplicationDetail}
   */
  @GET
  @Path("/app/{app-id}")
  public void getAppDetail(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace,
      @PathParam("app-id") String application) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    responder.sendJson(HttpResponseStatus.OK,
        GSON.toJson(applicationLifecycleService.getLatestAppDetail(
            new ApplicationReference(namespaceId, application))));
  }

  /**
   * Get {@link ApplicationDetail} for a given application.
   *
   * @param request {@link HttpRequest}
   * @param responder {@link HttpResponse}
   * @param namespace the namespace to get all application details
   * @param application the id of the application to get its {@link ApplicationDetail}
   * @throws Exception if either namespace or application doesn't exist, or failed to get {@link
   *     ApplicationDetail}
   */
  @GET
  @Path("/app/{app-id}/versions/{version-id}")
  public void getAppDetailForVersion(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespace,
      @PathParam("app-id") final String application,
      @PathParam("version-id") final String version) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    ApplicationId appId = new ApplicationId(namespace, application, version);
    ApplicationDetail appDetail = ApplicationId.DEFAULT_VERSION.equals(version)
        ? applicationLifecycleService.getLatestAppDetail(appId.getAppReference())
        : applicationLifecycleService.getAppDetail(appId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(appDetail));
  }

  /**
   * Deploy an application. This is similar to the public API to deploy an application.
   * This differs from the public API by supporting the skipMakingLatest parameter.
   * This behaviour (skipMakingLatest) should not be exposed in the public API and should
   * only be used internally when needed.
   *
   * @param request {@link HttpRequest}
   * @param responder {@link HttpResponder}
   * @param namespaceId of the namespace where the app is to be deployed
   * @param appId of the app
   * @param skipMarkingLatest if true, the app will be deployed but not marked latest.
   *         The version of the application that is marked as latest will be run, when an application run
   *         is triggered. If the application is not marked as latest during deployment (i.e. when
   *         the skipMarkingLatest param is true), the version of the application that gets deployed
   *         with this API will not be used when the application run is triggered.
   * @return {@link BodyConsumer}
   * @throws BadRequestException when the request params or body are not valid
   * @throws NamespaceNotFoundException when the namespace is not found
   * @throws AccessException in case of any security issues
   * @throws UnsupportedOperationException when PIPELINE_PROMOTION_TO_PRODUCTION feature is not enabled
   */
  @PUT
  @Path("/apps/{app-id}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public BodyConsumer create(HttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespaceId,
      @PathParam("app-id") final String appId,
      @QueryParam("skipMarkingLatest") final boolean skipMarkingLatest) throws Exception {

    String versionId = ApplicationId.DEFAULT_VERSION;
    if (Feature.LIFECYCLE_MANAGEMENT_EDIT.isEnabled(featureFlagsProvider)) {
      versionId = RunIds.generate().getId();
    }
    ApplicationId applicationId = validateApplicationVersionId(validateNamespace(namespaceId), appId, versionId);

    return deployAppFromArtifact(applicationId, skipMarkingLatest);
  }

  /**
   * Mark provided application versions as latest.
   *
   * @param request {@link FullHttpRequest}
   * @param responder {@link HttpResponse}
   * @param namespace the namespace where the applications are deployed
   * @throws Exception if either namespace or any of the application (versions) doesn't exist
   */
  @POST
  @Path("/apps/markLatest")
  public void markApplicationsAsLatest(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespace) throws Exception {

    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }

    MarkLatestAppsRequest appsMarkLatestRequest;
    try {
      appsMarkLatestRequest = parseBody(request, MarkLatestAppsRequest.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid request body", e);
    }

    if (appsMarkLatestRequest == null) {
      throw new BadRequestException("Invalid request body.");
    }

    applicationLifecycleService.markAppsAsLatest(namespaceId, appsMarkLatestRequest);
    responder.sendString(HttpResponseStatus.OK, "");
  }

  /**
   * Update git metadata of the provided applications.
   *
   * @param request {@link FullHttpRequest}
   * @param responder {@link HttpResponse}
   * @param namespace the namespace where the applications are deployed
   * @throws Exception if either namespace or any of the application doesn't exist
   */
  @POST
  @Path("/apps/updateSourceControlMeta")
  public void updateSourceControlMeta(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") final String namespace) throws Exception {
    // TODO Add the feature flag check here, after the markLatest API PR is merged and this branch is synced.
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }

    UpdateMultiSourceControlMetaReqeust appsRequest;
    try {
      appsRequest = parseBody(request, UpdateMultiSourceControlMetaReqeust.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid request body.", e);
    }

    if (null == appsRequest) {
      throw new BadRequestException("Empty request body.");
    }

    applicationLifecycleService.updateSourceControlMeta(namespaceId, appsRequest);
    responder.sendString(HttpResponseStatus.OK, "");
  }
}

/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.artifact.ApplicationClass;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ArtifactAlreadyExistsException;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.CannotBeDeletedException;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.AbstractBodyConsumer;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDetail;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.WriteConflictException;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.DatasetDetail;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.artifact.CreateAppRequest;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

/**
 * {@link co.cask.http.HttpHandler} for managing application lifecycle.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppLifecycleHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(AppLifecycleHttpHandler.class);

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final CConfiguration configuration;
  private final ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory;
  private final Scheduler scheduler;
  private final NamespaceAdmin namespaceAdmin;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final AdapterService adapterService;
  private final ArtifactRepository artifactRepository;
  private final File tmpDir;

  @Inject
  public AppLifecycleHttpHandler(CConfiguration configuration,
                                 ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory,
                                 Scheduler scheduler, ProgramRuntimeService runtimeService, Store store,
                                 NamespaceAdmin namespaceAdmin, NamespacedLocationFactory namespacedLocationFactory,
                                 ApplicationLifecycleService applicationLifecycleService,
                                 AdapterService adapterService,
                                 ArtifactRepository artifactRepository) {
    this.configuration = configuration;
    this.managerFactory = managerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.scheduler = scheduler;
    this.runtimeService = runtimeService;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.store = store;
    this.applicationLifecycleService = applicationLifecycleService;
    this.adapterService = adapterService;
    this.artifactRepository = artifactRepository;
    this.tmpDir = new File(new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR)),
                           configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  /**
   * Deploys an application with the specified name.
   */
  @PUT
  @Path("/apps/{app-id}")
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId,
                             @PathParam("app-id") final String appId,
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName,
                             @HeaderParam(APP_CONFIG_HEADER) String configString,
                             @HeaderParam(HttpHeaders.Names.CONTENT_TYPE) String contentType) {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);

    // yes this is weird... here for backwards compatibility. If content type is json, use the preferred method
    // of creating an app from an artifact. Otherwise use the old method where the body is the jar contents.
    boolean createFromArtifact = MediaType.APPLICATION_JSON.equals(contentType);

    try {
      if (createFromArtifact) {
        return deployAppFromArtifact(namespace, appId);
      } else {
        return deployApplication(responder, namespaceId, appId, archiveName, configString);
      }
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
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId,
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName,
                             @HeaderParam(APP_CONFIG_HEADER) String configString) {
    // null means use name provided by app spec
    try {
      return deployApplication(responder, namespaceId, null, archiveName, configString);
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
                         @PathParam("namespace-id") String namespaceId) throws NamespaceNotFoundException {
    getAppRecords(responder, store, namespaceId);
  }

  /**
   * Returns the info associated with the application.
   */
  @GET
  @Path("/apps/{app-id}")
  public void getAppInfo(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("app-id") final String appId) {
    getAppDetails(responder, namespaceId, appId);
  }

  /**
   * Delete an application specified by appId.
   */
  @DELETE
  @Path("/apps/{app-id}")
  public void deleteApp(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-id") final String appId) throws Exception {

    Id.Application id = Id.Application.from(namespaceId, appId);
    // Deletion of a particular application is not allowed if that application is used by an adapter
    AppFabricServiceStatus appStatus;
    if (!adapterService.canDeleteApp(id)) {
      appStatus = AbstractAppFabricHttpHandler.AppFabricServiceStatus.ADAPTER_CONFLICT;
    } else {
      try {
        applicationLifecycleService.removeApplication(id);
        appStatus = AppFabricServiceStatus.OK;
      } catch (NotFoundException nfe) {
        appStatus = AppFabricServiceStatus.PROGRAM_NOT_FOUND;
      } catch (CannotBeDeletedException cbde) {
        appStatus = AppFabricServiceStatus.PROGRAM_STILL_RUNNING;
      } catch (Exception e) {
        appStatus = AppFabricServiceStatus.INTERNAL_ERROR;
      }
    }
    LOG.trace("Delete call for Application {} at AppFabricHttpHandler", appId);
    responder.sendString(appStatus.getCode(), appStatus.getMessage());
  }

  /**
   * Deletes all applications in CDAP.
   */
  @DELETE
  @Path("/apps")
  public void deleteAllApps(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {

    Id.Namespace id = Id.Namespace.from(namespaceId);
    AppFabricServiceStatus status;
    try {
      applicationLifecycleService.removeAll(id);
      status = AppFabricServiceStatus.OK;
    } catch (NotFoundException nfe) {
      status = AppFabricServiceStatus.PROGRAM_NOT_FOUND;
    } catch (CannotBeDeletedException cbde) {
      status = AppFabricServiceStatus.PROGRAM_STILL_RUNNING;
    } catch (Exception e) {
      status = AppFabricServiceStatus.INTERNAL_ERROR;
    }
    LOG.trace("Delete all call at AppFabricHttpHandler");
    responder.sendString(status.getCode(), status.getMessage());
  }

  // normally we wouldn't want to use a body consumer but would just want to read the request body directly
  // since it wont be big. But the deploy app API has one path with different behavior based on content type
  // the other behavior requires a BodyConsumer and only have one method per path is allowed,
  // so we have to use a BodyConsumer
  private BodyConsumer deployAppFromArtifact(final Id.Namespace namespace, final String appId) throws IOException {

    return new AbstractBodyConsumer(File.createTempFile(appId, ".json", tmpDir)) {

      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) {
        try (FileReader fileReader = new FileReader(uploadedFile)) {

          CreateAppRequest<?> createAppRequest = GSON.fromJson(fileReader, CreateAppRequest.class);
          ArtifactSummary artifactSummary = createAppRequest.getArtifact();
          Id.Namespace artifactNamespace = artifactSummary.isSystem() ? Id.Namespace.SYSTEM : namespace;
          Id.Artifact artifactId =
            Id.Artifact.from(artifactNamespace, artifactSummary.getName(), artifactSummary.getVersion());

          ArtifactDetail artifactDetail;
          try {
            artifactDetail = artifactRepository.getArtifact(artifactId);
          } catch (ArtifactNotFoundException e) {
            responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
            return;
          }

          Set<ApplicationClass> appClasses = artifactDetail.getMeta().getClasses().getApps();
          if (appClasses.isEmpty()) {
            responder.sendString(HttpResponseStatus.BAD_REQUEST, String.format(
              "No application classes found in artifact %s.", artifactId));
            return;
          }
          String className = appClasses.iterator().next().getClassName();
          Location location = artifactDetail.getDescriptor().getLocation();

          // if we don't null check, it get serialized to "null"
          String configString = createAppRequest.getConfig() == null ? null : GSON.toJson(createAppRequest.getConfig());
          AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(artifactId, className, location, configString);
          deploy(namespace.getId(), appId, deploymentInfo);
          responder.sendString(HttpResponseStatus.OK, "Deploy Complete");
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
                                         final String namespaceId,
                                         final String appId,
                                         final String archiveName,
                                         final String configString) throws IOException {
    final Id.Namespace namespace = Id.Namespace.from(namespaceId);
    if (!namespaceAdmin.hasNamespace(namespace)) {
      LOG.warn("Namespace '{}' not found.", namespaceId);
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           String.format("Deploy failed - namespace '%s' not found.", namespaceId));
      return null;
    }

    Location namespaceHomeLocation = namespacedLocationFactory.get(namespace);
    if (!namespaceHomeLocation.exists()) {
      String msg = String.format("Home directory %s for namespace %s not found",
                                 namespaceHomeLocation.toURI().getPath(), namespaceId);
      LOG.error(msg);
      responder.sendString(HttpResponseStatus.NOT_FOUND, msg);
      return null;
    }

    if (archiveName == null || archiveName.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, ARCHIVE_NAME_HEADER + " header not present",
                           ImmutableMultimap.of(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE));
      return null;
    }

    // TODO: (CDAP-3258) error handling needs to be refactored here, should be able just to throw the exception,
    // but the caller catches all exceptions and responds with a 500
    final Id.Artifact artifactId;
    try {
      artifactId = ArtifactRepository.parse(namespace, archiveName);
    } catch (InvalidArtifactException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      return null;
    }

    // Store uploaded content to a local temp file
    String namespacesDir = configuration.get(Constants.Namespace.NAMESPACES_DIR);
    File localDataDir = new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR));
    File namespaceBase = new File(localDataDir, namespacesDir);
    File tempDir = new File(new File(namespaceBase, namespaceId),
                            configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException("Could not create temporary directory at: " + tempDir);
    }

    return new AbstractBodyConsumer(File.createTempFile("app-", ".jar", tempDir)) {

      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) {
        try {
          // add artifact to ArtifactRepository
          ArtifactDetail artifactDetail = artifactRepository.addArtifact(artifactId, uploadedFile);

          Set<ApplicationClass> appClasses = artifactDetail.getMeta().getClasses().getApps();
          if (appClasses.isEmpty()) {
            responder.sendString(HttpResponseStatus.BAD_REQUEST, "No application classes found in jar.");
            return;
          }
          String className = appClasses.iterator().next().getClassName();
          Location location = artifactDetail.getDescriptor().getLocation();

          // deploy application with newly added artifact
          AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(artifactId, className, location, configString);
          deploy(namespaceId, appId, deploymentInfo);
          responder.sendString(HttpResponseStatus.OK, "Deploy Complete");
        } catch (InvalidArtifactException e) {
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        } catch (ArtifactAlreadyExistsException e) {
          responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
        } catch (WriteConflictException e) {
          // don't really expect this to happen. It means after multiple retries there were still write conflicts.
          LOG.warn("Write conflict while trying to add artifact {}.", artifactId, e);
          responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                               "Write conflict while adding artifact. This can happen if multiple requests to add " +
                               "the same artifact occur simultaneously. Please try again.");
        } catch (Exception e) {
          LOG.error("Deploy failure", e);
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        }
      }
    };
  }

  // deploy helper
  private void deploy(final String namespaceId, final String appId, AppDeploymentInfo deploymentInfo) throws Exception {
    try {
      Id.Namespace id = Id.Namespace.from(namespaceId);

      Manager<AppDeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(new ProgramTerminator() {
        @Override
        public void stop(Id.Program programId) throws ExecutionException {
          deleteHandler(programId);
        }
      });

      manager.deploy(id, appId, deploymentInfo).get();
    } catch (Throwable e) {
      LOG.warn(e.getMessage(), e);
      throw new Exception(e.getMessage());
    }
  }

  private void deleteHandler(Id.Program programId) throws ExecutionException {
    try {
      switch (programId.getType()) {
        case FLOW:
          stopProgramIfRunning(programId);
          break;
        case WORKFLOW:
          scheduler.deleteSchedules(programId, SchedulableProgramType.WORKFLOW);
          break;
        case MAPREDUCE:
          //no-op
          break;
        case SERVICE:
          stopProgramIfRunning(programId);
          break;
        case WORKER:
          stopProgramIfRunning(programId);
          break;
      }
    } catch (InterruptedException e) {
      throw new ExecutionException(e);
    } catch (SchedulerException e) {
      throw new ExecutionException(e);
    }
  }

  private void stopProgramIfRunning(Id.Program programId)
    throws InterruptedException, ExecutionException {
    ProgramRuntimeService.RuntimeInfo programRunInfo = findRuntimeInfo(programId.getNamespaceId(),
                                                                       programId.getApplicationId(),
                                                                       programId.getId(),
                                                                       programId.getType(),
                                                                       runtimeService);
    if (programRunInfo != null) {
      doStop(programRunInfo);
    }
  }

  private void doStop(ProgramRuntimeService.RuntimeInfo runtimeInfo)
    throws ExecutionException, InterruptedException {
    Preconditions.checkNotNull(runtimeInfo, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));
    ProgramController controller = runtimeInfo.getController();
    controller.stop().get();
  }

  private void getAppDetails(HttpResponder responder, String namespace, String name) {
    try {
      ApplicationSpecification appSpec = store.getApplication(new Id.Application(Id.Namespace.from(namespace), name));
      if (appSpec == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
      ApplicationDetail appDetail = makeAppDetail(appSpec);
      responder.sendJson(HttpResponseStatus.OK, appDetail);
    } catch (SecurityException e) {
      LOG.debug("Security Exception while retrieving app details: ", e);
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception : ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private static ApplicationDetail makeAppDetail(ApplicationSpecification spec) {
    List<ProgramRecord> programs = Lists.newArrayList();
    for (ProgramSpecification programSpec : spec.getFlows().values()) {
      programs.add(new ProgramRecord(ProgramType.FLOW, spec.getName(),
                                     programSpec.getName(), programSpec.getDescription()));
    }
    for (ProgramSpecification programSpec : spec.getMapReduce().values()) {
      programs.add(new ProgramRecord(ProgramType.MAPREDUCE, spec.getName(),
                                     programSpec.getName(), programSpec.getDescription()));
    }
    for (ProgramSpecification programSpec : spec.getServices().values()) {
      programs.add(new ProgramRecord(ProgramType.SERVICE, spec.getName(),
                                     programSpec.getName(), programSpec.getDescription()));
    }
    for (ProgramSpecification programSpec : spec.getSpark().values()) {
      programs.add(new ProgramRecord(ProgramType.SPARK, spec.getName(),
                                     programSpec.getName(), programSpec.getDescription()));
    }
    for (ProgramSpecification programSpec : spec.getWorkers().values()) {
      programs.add(new ProgramRecord(ProgramType.WORKER, spec.getName(),
                                     programSpec.getName(), programSpec.getDescription()));
    }
    for (ProgramSpecification programSpec : spec.getWorkflows().values()) {
      programs.add(new ProgramRecord(ProgramType.WORKFLOW, spec.getName(),
                                     programSpec.getName(), programSpec.getDescription()));
    }

    List<StreamDetail> streams = Lists.newArrayList();
    for (StreamSpecification streamSpec : spec.getStreams().values()) {
      streams.add(new StreamDetail(streamSpec.getName()));
    }

    List<DatasetDetail> datasets = Lists.newArrayList();
    for (DatasetCreationSpec datasetSpec : spec.getDatasets().values()) {
      datasets.add(new DatasetDetail(datasetSpec.getInstanceName(), datasetSpec.getTypeName()));
    }

    return new ApplicationDetail(spec.getName(), spec.getVersion(), spec.getDescription(), spec.getConfiguration(),
                                 streams, datasets, programs);
  }
}

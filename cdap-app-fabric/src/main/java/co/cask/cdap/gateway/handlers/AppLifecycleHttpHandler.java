/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.discovery.TimeLimitEndpointStrategy;
import co.cask.cdap.common.http.AbstractBodyConsumer;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.OperationException;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProgramTypes;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} for managing application lifecycle.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppLifecycleHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppLifecycleHttpHandler.class);

  /**
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

  /**
   * Timeout to get response from metrics system.
   */
  private static final long METRICS_SERVER_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  /**
   * Number of seconds for timing out a service endpoint discovery.
   */
  private static final long DISCOVERY_TIMEOUT_SECONDS = 3;

  /**
   * Configuration object passed from higher up.
   */
  private final CConfiguration configuration;

  private final ManagerFactory<Location, ApplicationWithPrograms> managerFactory;

  /**
   * App fabric output directory.
   */
  private final String appFabricDir;

  /**
   * The directory where the uploaded files would be placed.
   */
  private final String archiveDir;

  /**
   * Factory for handling the location - can do both in either Distributed or Local mode.
   */
  private final LocationFactory locationFactory;

  private final Scheduler scheduler;

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final StreamConsumerFactory streamConsumerFactory;

  private final QueueAdmin queueAdmin;

  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public AppLifecycleHttpHandler(Authenticator authenticator, CConfiguration configuration,
                                 ManagerFactory<Location, ApplicationWithPrograms> managerFactory,
                                 LocationFactory locationFactory, Scheduler scheduler,
                                 ProgramRuntimeService runtimeService, StoreFactory storeFactory,
                                 StreamConsumerFactory streamConsumerFactory, QueueAdmin queueAdmin,
                                 DiscoveryServiceClient discoveryServiceClient) {
    super(authenticator);
    this.configuration = configuration;
    this.managerFactory = managerFactory;
    this.appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR);
    this.archiveDir = this.appFabricDir + "/archive";
    this.locationFactory = locationFactory;
    this.scheduler = scheduler;
    this.runtimeService = runtimeService;
    this.store = storeFactory.create();
    this.streamConsumerFactory = streamConsumerFactory;
    this.queueAdmin = queueAdmin;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  /**
   * Deploys an application with the specified name.
   */
  @PUT
  @Path("/apps/{app-id}")
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId,
                             @PathParam("app-id") final String appId,
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName) {
    try {
      return deployApplication(request, responder, namespaceId, appId, archiveName);
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
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName) {
    // null means use name provided by app spec
    try {
      return deployApplication(request, responder, namespaceId, null, archiveName);
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
                         @PathParam("namespace-id") String namespaceId) {
    getAppDetails(responder, namespaceId, null);
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
                        @PathParam("app-id") final String appId) {
    try {
      Id.Program id = Id.Program.from(namespaceId, appId, "");
      AppFabricServiceStatus appStatus = removeApplication(id);
      LOG.trace("Delete call for Application {} at AppFabricHttpHandler", appId);
      responder.sendString(appStatus.getCode(), appStatus.getMessage());
    } catch (SecurityException e) {
      LOG.debug("Security Exception while deleting app: ", e);
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Deletes all applications in CDAP.
   */
  @DELETE
  @Path("/apps")
  public void deleteAllApps(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) {
    try {
      Id.Account id = Id.Account.from(namespaceId);
      AppFabricServiceStatus status = removeAll(id);
      LOG.trace("Delete all call at AppFabricHttpHandler");
      responder.sendString(status.getCode(), status.getMessage());
    } catch (SecurityException e) {
      LOG.debug("Security Exception while deleting all apps: ", e);
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private BodyConsumer deployApplication(final HttpRequest request, final HttpResponder responder,
                                         final String namespaceId, final String appId,
                                         final String archiveName) throws IOException {
    if (!namespaceExists(namespaceId)) {
      LOG.warn("Deploy failed - namespace '{}' does not exist.", namespaceId);
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Deploy failed - namespace '%s' does not " +
                                                                         "exist.", namespaceId));
      return null;
    }

    if (archiveName == null || archiveName.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, ARCHIVE_NAME_HEADER + " header not present",
                           ImmutableMultimap.of(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE));
      return null;
    }

    // Store uploaded content to a local temp file
    File tempDir = new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR),
                            configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException("Could not create temporary directory at: " + tempDir);
    }

    final Location archiveDir = locationFactory.create(this.archiveDir).append(namespaceId);
    final Location archive = archiveDir.append(archiveName);

    return new AbstractBodyConsumer(File.createTempFile("app-", ".jar", tempDir)) {

      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) {
        try {
          Locations.mkdirsIfNotExists(archiveDir);

          // Copy uploaded content to a temporary location
          Location tmpLocation = archive.getTempFile(".tmp");
          try {
            LOG.debug("Copy from {} to {}", uploadedFile, tmpLocation.toURI());
            Files.copy(uploadedFile, Locations.newOutputSupplier(tmpLocation));

            // Finally, move archive to final location
            if (tmpLocation.renameTo(archive) == null) {
              throw new IOException(String.format("Could not move archive from location: %s, to location: %s",
                                                  tmpLocation.toURI(), archive.toURI()));
            }
          } catch (IOException e) {
            // In case copy to temporary file failed, or rename failed
            tmpLocation.delete();
            throw e;
          }

          deploy(namespaceId, appId, archive);
          responder.sendString(HttpResponseStatus.OK, "Deploy Complete");
        } catch (Exception e) {
          LOG.error("Deploy failure", e);
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        }
      }
    };
  }

  // deploy helper
  private void deploy(final String namespaceId, final String appId , Location archive) throws Exception {
    try {
      Id.Account id = Id.Account.from(namespaceId);
      Location archiveLocation = archive;
      Manager<Location, ApplicationWithPrograms> manager = managerFactory.create(new ProgramTerminator() {
        @Override
        public void stop(Id.Account id, Id.Program programId, ProgramType type) throws ExecutionException {
          deleteHandler(programId, type);
        }
      });

      ApplicationWithPrograms applicationWithPrograms =
        manager.deploy(id, appId, archiveLocation).get();
      ApplicationSpecification specification = applicationWithPrograms.getSpecification();
      setupSchedules(namespaceId, specification);
    } catch (Throwable e) {
      LOG.warn(e.getMessage(), e);
      throw new Exception(e.getMessage());
    }
  }

  private boolean namespaceExists(String namespace) {
    boolean isValid = false;
    if (namespace != null && !namespace.isEmpty()) {
      if (store.getNamespace(Id.Namespace.from(namespace)) != null) {
        isValid = true;
      } else {
        // null namespace found. check if this is default, and try to create it.
        // This logic could be changed later to figure out a way to create the 'default' namespace at a better
        // time during the initialization of CDAP.
        if (Constants.DEFAULT_NAMESPACE.equals(namespace)) {
          NamespaceMeta existing = store.createNamespace(new NamespaceMeta.Builder()
                                                           .setId(Constants.DEFAULT_NAMESPACE)
                                                           .setDisplayName(Constants.DEFAULT_NAMESPACE)
                                                           .setDescription(Constants.DEFAULT_NAMESPACE).build());
          if (existing != null) {
            LOG.trace("Default namespace already exists.");
          } else {
            LOG.trace("Created default namespace.");
          }
          isValid = true;
        }
      }
    }
    return isValid;
  }

  private void deleteHandler(Id.Program programId, ProgramType type)
    throws ExecutionException {
    try {
      switch (type) {
        case FLOW:
          stopProgramIfRunning(programId, type);
          break;
        case PROCEDURE:
          stopProgramIfRunning(programId, type);
          break;
        case WORKFLOW:
          List<String> scheduleIds = scheduler.getScheduleIds(programId, type);
          scheduler.deleteSchedules(programId, ProgramType.WORKFLOW, scheduleIds);
          break;
        case MAPREDUCE:
          //no-op
          break;
        case SERVICE:
          stopProgramIfRunning(programId, type);
          break;
      }
    } catch (InterruptedException e) {
      throw new ExecutionException(e);
    }
  }

  private void setupSchedules(String namespaceId, ApplicationSpecification specification)  throws IOException {

    for (Map.Entry<String, WorkflowSpecification> entry : specification.getWorkflows().entrySet()) {
      Id.Program programId = Id.Program.from(namespaceId, specification.getName(), entry.getKey());
      List<String> existingSchedules = scheduler.getScheduleIds(programId, ProgramType.WORKFLOW);
      //Delete the existing schedules and add new ones.
      if (!existingSchedules.isEmpty()) {
        scheduler.deleteSchedules(programId, ProgramType.WORKFLOW, existingSchedules);
      }
      // Add new schedules.
      if (!entry.getValue().getSchedules().isEmpty()) {
        scheduler.schedule(programId, ProgramType.WORKFLOW, entry.getValue().getSchedules());
      }
    }
  }

  private void stopProgramIfRunning(Id.Program programId, ProgramType type)
    throws InterruptedException, ExecutionException {
    ProgramRuntimeService.RuntimeInfo programRunInfo = findRuntimeInfo(programId.getAccountId(),
                                                                       programId.getApplicationId(),
                                                                       programId.getId(),
                                                                       type, runtimeService);
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

  private void getAppDetails(HttpResponder responder, String namespaceId, String appId) {
    if (appId != null && appId.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "app-id is empty");
      return;
    }

    try {
      Id.Account accId = Id.Account.from(namespaceId);
      List<ApplicationRecord> result = Lists.newArrayList();
      List<ApplicationSpecification> specList;
      if (appId == null) {
        specList = new ArrayList<ApplicationSpecification>(store.getAllApplications(accId));
      } else {
        ApplicationSpecification appSpec = store.getApplication(new Id.Application(accId, appId));
        if (appSpec == null) {
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          return;
        }
        specList = Collections.singletonList(store.getApplication(new Id.Application(accId, appId)));
      }

      for (ApplicationSpecification appSpec : specList) {
        result.add(makeAppRecord(appSpec));
      }

      String json;
      if (appId == null) {
        json = GSON.toJson(result);
      } else {
        json = GSON.toJson(result.get(0));
      }

      responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                              ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
    } catch (SecurityException e) {
      LOG.debug("Security Exception while retrieving app details: ", e);
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception : ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Protected only to support v2 APIs
   */
  protected AppFabricServiceStatus removeAll(Id.Account identifier) throws Exception {
    List<ApplicationSpecification> allSpecs = new ArrayList<ApplicationSpecification>(
      store.getAllApplications(identifier));

    //Check if any App associated with this namespace is running
    final Id.Account accId = Id.Account.from(identifier.getId());
    boolean appRunning = checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.getApplication().getAccount().equals(accId);
      }
    }, ProgramType.values());

    if (appRunning) {
      return AppFabricServiceStatus.PROGRAM_STILL_RUNNING;
    }

    //All Apps are STOPPED, delete them
    for (ApplicationSpecification appSpec : allSpecs) {
      Id.Program id = Id.Program.from(identifier.getId(), appSpec.getName() , "");
      removeApplication(id);
    }
    return AppFabricServiceStatus.OK;
  }

  private AppFabricServiceStatus removeApplication(Id.Program identifier) throws Exception {
    Id.Account accountId = Id.Account.from(identifier.getAccountId());
    final Id.Application appId = Id.Application.from(accountId, identifier.getApplicationId());

    //Check if all are stopped.
    boolean appRunning = checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.getApplication().equals(appId);
      }
    }, ProgramType.values());

    if (appRunning) {
      return AppFabricServiceStatus.PROGRAM_STILL_RUNNING;
    }

    ApplicationSpecification spec = store.getApplication(appId);
    if (spec == null) {
      return AppFabricServiceStatus.PROGRAM_NOT_FOUND;
    }

    //Delete the schedules
    for (WorkflowSpecification workflowSpec : spec.getWorkflows().values()) {
      Id.Program workflowProgramId = Id.Program.from(appId, workflowSpec.getName());
      List<String> schedules = scheduler.getScheduleIds(workflowProgramId, ProgramType.WORKFLOW);
      if (!schedules.isEmpty()) {
        scheduler.deleteSchedules(workflowProgramId, ProgramType.WORKFLOW, schedules);
      }
    }

    deleteMetrics(identifier.getAccountId(), identifier.getApplicationId());

    // Delete all streams and queues state of each flow
    // TODO: This should be unified with the DeletedProgramHandlerStage
    for (FlowSpecification flowSpecification : spec.getFlows().values()) {
      Id.Program flowProgramId = Id.Program.from(appId, flowSpecification.getName());

      // Collects stream name to all group ids consuming that stream
      Multimap<String, Long> streamGroups = HashMultimap.create();
      for (FlowletConnection connection : flowSpecification.getConnections()) {
        if (connection.getSourceType() == FlowletConnection.Type.STREAM) {
          long groupId = FlowUtils.generateConsumerGroupId(flowProgramId, connection.getTargetName());
          streamGroups.put(connection.getSourceName(), groupId);
        }
      }
      // Remove all process states and group states for each stream
      String namespace = String.format("%s.%s", flowProgramId.getApplicationId(), flowProgramId.getId());
      for (Map.Entry<String, Collection<Long>> entry : streamGroups.asMap().entrySet()) {
        streamConsumerFactory.dropAll(QueueName.fromStream(entry.getKey()), namespace, entry.getValue());
      }

      queueAdmin.dropAllForFlow(identifier.getApplicationId(), flowSpecification.getName());
    }
    deleteProgramLocations(appId);

    Location appArchive = store.getApplicationArchiveLocation(appId);
    Preconditions.checkNotNull(appArchive, "Could not find the location of application", appId.getId());
    appArchive.delete();
    store.removeApplication(appId);
    return AppFabricServiceStatus.OK;
  }

  /**
   * Temporarily protected only to support v2 APIs. Currently used in unrecoverable/reset. Should become private once
   * the reset API has a v3 version
   */
  protected void deleteMetrics(String namespaceId, String applicationId) throws IOException, OperationException {
    Collection<ApplicationSpecification> applications = Lists.newArrayList();
    if (applicationId == null) {
      applications = this.store.getAllApplications(new Id.Account(namespaceId));
    } else {
      ApplicationSpecification spec = this.store.getApplication
        (new Id.Application(new Id.Account(namespaceId), applicationId));
      applications.add(spec);
    }
    Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(Constants.Service.METRICS);
    Discoverable discoverable = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoverables),
                                                              DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS).pick();

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      throw new IOException("Can't find Metrics endpoint");
    }

    for (MetricsScope scope : MetricsScope.values()) {
      for (ApplicationSpecification application : applications) {
        String url = String.format("http://%s:%d%s/metrics/%s/apps/%s",
                                   discoverable.getSocketAddress().getHostName(),
                                   discoverable.getSocketAddress().getPort(),
                                   Constants.Gateway.API_VERSION_2,
                                   scope.name().toLowerCase(),
                                   application.getName());
        sendMetricsDelete(url);
      }
    }

    if (applicationId == null) {
      String url = String.format("http://%s:%d%s/metrics", discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort(), Constants.Gateway.API_VERSION_2);
      sendMetricsDelete(url);
    }
  }

  private void sendMetricsDelete(String url) {
    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) METRICS_SERVER_RESPONSE_TIMEOUT)
      .build();

    try {
      client.delete().get(METRICS_SERVER_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error("exception making metrics delete call", e);
      Throwables.propagate(e);
    } finally {
      client.close();
    }
  }

  /**
   * Delete the jar location of the program.
   *
   * @param appId        applicationId.
   * @throws IOException if there are errors with location IO
   */
  private void deleteProgramLocations(Id.Application appId) throws IOException, OperationException {
    ApplicationSpecification specification = store.getApplication(appId);

    Iterable<ProgramSpecification> programSpecs = Iterables.concat(specification.getFlows().values(),
                                                                   specification.getMapReduce().values(),
                                                                   specification.getProcedures().values(),
                                                                   specification.getWorkflows().values());

    for (ProgramSpecification spec : programSpecs) {
      ProgramType type = ProgramTypes.fromSpecification(spec);
      Id.Program programId = Id.Program.from(appId, spec.getName());
      try {
        Location location = Programs.programLocation(locationFactory, appFabricDir, programId, type);
        location.delete();
      } catch (FileNotFoundException e) {
        LOG.warn("Program jar for program {} not found.", programId.toString(), e);
      }
    }

    // Delete webapp
    // TODO: this will go away once webapp gets a spec
    try {
      Id.Program programId = Id.Program.from(appId.getAccountId(), appId.getId(),
                                             ProgramType.WEBAPP.name().toLowerCase());
      Location location = Programs.programLocation(locationFactory, appFabricDir, programId, ProgramType.WEBAPP);
      location.delete();
    } catch (FileNotFoundException e) {
      // expected exception when webapp is not present.
    }
  }

  /**
   * Check if any program that satisfy the given {@link Predicate} is running.
   * Protected only to support v2 APIs
   *
   * @param predicate Get call on each running {@link Id.Program}.
   * @param types Types of program to check
   * returns True if a program is running as defined by the predicate.
   */
  protected boolean checkAnyRunning(Predicate<Id.Program> predicate, ProgramType... types) {
    for (ProgramType type : types) {
      for (Map.Entry<RunId, ProgramRuntimeService.RuntimeInfo> entry :  runtimeService.list(type).entrySet()) {
        ProgramController.State programState = entry.getValue().getController().getState();
        if (programState == ProgramController.State.STOPPED || programState == ProgramController.State.ERROR) {
          continue;
        }
        Id.Program programId = entry.getValue().getProgramId();
        if (predicate.apply(programId)) {
          LOG.trace("Program still running in checkAnyRunning: {} {} {} {}",
                    programId.getApplicationId(), type, programId.getId(), entry.getValue().getController().getRunId());
          return true;
        }
      }
    }
    return false;
  }

  private static ApplicationRecord makeAppRecord(ApplicationSpecification appSpec) {
    return new ApplicationRecord("App", appSpec.getName(), appSpec.getName(), appSpec.getDescription());
  }
}

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
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.schedule.SchedulableProgramType;
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
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.http.AbstractBodyConsumer;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.adapter.AdapterAlreadyExistsException;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.adapter.AdapterTypeInfo;
import co.cask.cdap.internal.app.runtime.adapter.InvalidAdapterOperationException;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProgramTypes;
import co.cask.cdap.proto.Sink;
import co.cask.cdap.proto.Source;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
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
import java.util.Set;
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

  private final ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory;

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
  private final PreferencesStore preferencesStore;
  private final AdapterService adapterService;
  private final NamespaceAdmin namespaceAdmin;

  @Inject
  public AppLifecycleHttpHandler(Authenticator authenticator, CConfiguration configuration,
                                 ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory,
                                 LocationFactory locationFactory, Scheduler scheduler,
                                 ProgramRuntimeService runtimeService, StoreFactory storeFactory,
                                 StreamConsumerFactory streamConsumerFactory, QueueAdmin queueAdmin,
                                 DiscoveryServiceClient discoveryServiceClient, PreferencesStore preferencesStore,
                                 AdapterService adapterService, NamespaceAdmin namespaceAdmin) {
    super(authenticator);
    this.configuration = configuration;
    this.managerFactory = managerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.locationFactory = locationFactory;
    this.scheduler = scheduler;
    this.runtimeService = runtimeService;
    this.store = storeFactory.create();
    this.streamConsumerFactory = streamConsumerFactory;
    this.queueAdmin = queueAdmin;
    this.discoveryServiceClient = discoveryServiceClient;
    this.preferencesStore = preferencesStore;
    this.adapterService = adapterService;
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
      Id.Application id = Id.Application.from(namespaceId, appId);

      // Deletion of a particular application is not allowed if that application is used by an adapter
      if (adapterService.getAdapterTypeInfo(appId) != null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             String.format("Cannot delete Application %s." +
                                             " An AdapterType exists with a conflicting name.", appId));

        return;
      }

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
      Id.Namespace id = Id.Namespace.from(namespaceId);
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

  /**
   * Retrieves all adapters in a given namespace.
   */
  @GET
  @Path("/adapters")
  public void listAdapters(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) {
    if (!namespaceAdmin.hasNamespace(Id.Namespace.from(namespaceId))) {
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           String.format("Namespace '%s' does not exist.", namespaceId));
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, adapterService.getAdapters(namespaceId));
  }

  /**
   * Retrieves an adapter
   */
  @GET
  @Path("/adapters/{adapter-id}")
  public void getAdapter(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("adapter-id") String adapterName) {
    try {
      AdapterSpecification adapterSpec = adapterService.getAdapter(namespaceId, adapterName);
      responder.sendJson(HttpResponseStatus.OK, adapterSpec);
    } catch (AdapterNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    }
  }

  /**
   * Starts/stops an adapter
   */
  @POST
  @Path("/adapters/{adapter-id}/{action}")
  public void startStopAdapter(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("adapter-id") String adapterId,
                               @PathParam("action") String action) {
    try {
      if ("start".equals(action)) {
        adapterService.startAdapter(namespaceId, adapterId);
      } else if ("stop".equals(action)) {
        adapterService.stopAdapter(namespaceId, adapterId);
      } else {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             String.format("Invalid adapter action: %s. Possible actions: ['start', 'stop'].", action));
        return;
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (InvalidAdapterOperationException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
    } catch (SchedulerException e) {
      LOG.error("Scheduler error in namespace '{}' for adapter '{}' with action '{}'",
                namespaceId, adapterId, action, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    } catch (Throwable t) {
      LOG.error("Error in namespace '{}' for adapter '{}' with action '{}'", namespaceId, adapterId, action, t);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Retrieves the status of an adapter
   */
  @GET
  @Path("/adapters/{adapter-id}/status")
  public void getAdapterStatus(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("adapter-id") String adapterId) {
    try {
      responder.sendString(HttpResponseStatus.OK, adapterService.getAdapterStatus(namespaceId, adapterId).toString());
    } catch (AdapterNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    }
  }

  /**
   * Deletes an adapter
   */
  @DELETE
  @Path("/adapters/{adapter-id}")
  public void deleteAdapter(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("adapter-id") String adapterName) {
    try {
      adapterService.removeAdapter(namespaceId, adapterName);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (SchedulerException e) {
      LOG.error("Scheduler error in namespace '{}' for adapter '{}' with action '{}'",
                namespaceId, adapterName, "delete", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    } catch (Throwable t) {
      LOG.error("Error in namespace '{}' for adapter '{}' with action '{}'",
                namespaceId, adapterName, "delete", t);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Create an adapter.
   */
  @POST
  @Path("/adapters/{adapter-id}")
  public void createAdapter(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("adapter-id") String adapterName) {

    try {
      if (!namespaceAdmin.hasNamespace(Id.Namespace.from(namespaceId))) {
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             String.format("Create adapter failed - namespace '%s' does not exist.", namespaceId));
        return;
      }

      AdapterConfig config = parseBody(request, AdapterConfig.class);
      if (config == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Insufficient parameters to create adapter");
        return;
      }

      // Validate the adapter
      String adapterType = config.getType();
      AdapterTypeInfo adapterTypeInfo = adapterService.getAdapterTypeInfo(adapterType);
      if (adapterTypeInfo == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Adapter type %s not found", adapterType));
        return;
      }

      AdapterSpecification spec = convertToSpec(adapterName, config, adapterTypeInfo);
      adapterService.createAdapter(namespaceId, spec);
      responder.sendString(HttpResponseStatus.OK, String.format("Adapter: %s is created", adapterName));
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (AdapterAlreadyExistsException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
    } catch (Throwable th) {
      LOG.error("Failed to deploy adapter", th);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, th.getMessage());
    }
  }

  private AdapterSpecification convertToSpec(String name, AdapterConfig config, AdapterTypeInfo typeInfo) {
    Map<String, String> sourceProperties = Maps.newHashMap(typeInfo.getDefaultSourceProperties());
    if (config.source.properties != null) {
      sourceProperties.putAll(config.source.properties);
    }
    Set<Source> sources = ImmutableSet.of(
      new Source(config.source.name, typeInfo.getSourceType(), sourceProperties));
    Map<String, String> sinkProperties = Maps.newHashMap(typeInfo.getDefaultSinkProperties());
    if (config.sink.properties != null) {
      sinkProperties.putAll(config.sink.properties);
    }
    Set<Sink> sinks = ImmutableSet.of(
      new Sink(config.sink.name, typeInfo.getSinkType(), sinkProperties));
    Map<String, String> adapterProperties = Maps.newHashMap(typeInfo.getDefaultAdapterProperties());
    if (config.properties != null) {
      adapterProperties.putAll(config.properties);
    }
    return new AdapterSpecification(name, config.getType(), adapterProperties, sources, sinks);
  }

  private BodyConsumer deployApplication(final HttpRequest request, final HttpResponder responder,
                                         final String namespaceId, final String appId,
                                         final String archiveName) throws IOException {
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    if (!namespaceAdmin.hasNamespace(namespace)) {
      LOG.warn("Namespace '{}' not found.", namespaceId);
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           String.format("Deploy failed - namespace '%s' not found.", namespaceId));
      return null;
    }

    Location namespaceHomeLocation = locationFactory.create(namespaceId);
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

    // Store uploaded content to a local temp file
    String tempBase = String.format("%s/%s", configuration.get(Constants.CFG_LOCAL_DATA_DIR), namespaceId);
    File tempDir = new File(tempBase, configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException("Could not create temporary directory at: " + tempDir);
    }

    String appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR);
    // note: cannot create an appId subdirectory under the namespace directory here because appId could be null here
    final Location archive =
      namespaceHomeLocation.append(appFabricDir).append(Constants.ARCHIVE_DIR).append(archiveName);

    return new AbstractBodyConsumer(File.createTempFile("app-", ".jar", tempDir)) {

      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) {
        try {
          DeploymentInfo deploymentInfo = new DeploymentInfo(uploadedFile, archive);
          deploy(namespaceId, appId, deploymentInfo);
          responder.sendString(HttpResponseStatus.OK, "Deploy Complete");
        } catch (Exception e) {
          LOG.error("Deploy failure", e);
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        }
      }
    };
  }

  // deploy helper
  private void deploy(final String namespaceId, final String appId, DeploymentInfo deploymentInfo) throws Exception {
    try {
      Id.Namespace id = Id.Namespace.from(namespaceId);

      Manager<DeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(new ProgramTerminator() {
        @Override
        public void stop(Id.Namespace id, Id.Program programId, ProgramType type) throws ExecutionException {
          deleteHandler(programId, type);
        }
      });

      manager.deploy(id, appId, deploymentInfo).get();
    } catch (Throwable e) {
      LOG.warn(e.getMessage(), e);
      throw new Exception(e.getMessage());
    }
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
          scheduler.deleteSchedules(programId, SchedulableProgramType.WORKFLOW);
          break;
        case MAPREDUCE:
          //no-op
          break;
        case SERVICE:
          stopProgramIfRunning(programId, type);
          break;
        case WORKER:
          stopProgramIfRunning(programId, type);
          break;
      }
    } catch (InterruptedException e) {
      throw new ExecutionException(e);
    } catch (SchedulerException e) {
      throw new ExecutionException(e);
    }
  }

  private void stopProgramIfRunning(Id.Program programId, ProgramType type)
    throws InterruptedException, ExecutionException {
    ProgramRuntimeService.RuntimeInfo programRunInfo = findRuntimeInfo(programId.getNamespaceId(),
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
    controller.kill().get();
  }

  private void getAppDetails(HttpResponder responder, String namespaceId, String appId) {
    if (appId != null && appId.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "app-id is empty");
      return;
    }

    try {
      Id.Namespace accId = Id.Namespace.from(namespaceId);
      List<ApplicationRecord> appRecords = Lists.newArrayList();
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
        appRecords.add(makeAppRecord(appSpec));
      }

      if (appId == null) {
        responder.sendJson(HttpResponseStatus.OK, appRecords);
      } else {
        responder.sendJson(HttpResponseStatus.OK, appRecords.get(0));
      }
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
  protected AppFabricServiceStatus removeAll(Id.Namespace identifier) throws Exception {
    List<ApplicationSpecification> allSpecs = new ArrayList<ApplicationSpecification>(
      store.getAllApplications(identifier));

    //Check if any App associated with this namespace is running
    final Id.Namespace accId = Id.Namespace.from(identifier.getId());
    boolean appRunning = checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.getApplication().getNamespace().equals(accId);
      }
    }, ProgramType.values());

    if (appRunning) {
      return AppFabricServiceStatus.PROGRAM_STILL_RUNNING;
    }

    //All Apps are STOPPED, delete them
    for (ApplicationSpecification appSpec : allSpecs) {
      Id.Application id = Id.Application.from(identifier.getId(), appSpec.getName());
      removeApplication(id);
    }
    return AppFabricServiceStatus.OK;
  }

  private AppFabricServiceStatus removeApplication(final Id.Application appId) throws Exception {
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
      Id.Program workflowProgramId = Id.Program.from(appId, ProgramType.WORKFLOW, workflowSpec.getName());
      scheduler.deleteSchedules(workflowProgramId, SchedulableProgramType.WORKFLOW);
    }

    deleteMetrics(appId.getNamespaceId(), appId.getId());

    //Delete all preferences of the application and of all its programs
    deletePreferences(appId);

    // Delete all streams and queues state of each flow
    // TODO: This should be unified with the DeletedProgramHandlerStage
    for (FlowSpecification flowSpecification : spec.getFlows().values()) {
      Id.Program flowProgramId = Id.Program.from(appId, ProgramType.FLOW, flowSpecification.getName());

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
        streamConsumerFactory.dropAll(Id.Stream.from(appId.getNamespaceId(), entry.getKey()),
                                      namespace, entry.getValue());
      }

      queueAdmin.dropAllForFlow(appId.getNamespaceId(), appId.getId(), flowSpecification.getName());
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
  protected void deleteMetrics(String namespaceId, String applicationId) throws IOException {
    Collection<ApplicationSpecification> applications = Lists.newArrayList();
    if (applicationId == null) {
      applications = this.store.getAllApplications(new Id.Namespace(namespaceId));
    } else {
      ApplicationSpecification spec = this.store.getApplication
        (new Id.Application(new Id.Namespace(namespaceId), applicationId));
      applications.add(spec);
    }
    ServiceDiscovered discovered = discoveryServiceClient.discover(Constants.Service.METRICS);
    Discoverable discoverable = new RandomEndpointStrategy(discovered).pick(DISCOVERY_TIMEOUT_SECONDS,
                                                                            TimeUnit.SECONDS);

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      throw new IOException("Can't find Metrics endpoint");
    }

    for (ApplicationSpecification application : applications) {
      String url = String.format("http://%s:%d%s/metrics/%s/apps/%s",
                                 discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort(),
                                 Constants.Gateway.API_VERSION_2,
                                 "ignored",
                                 application.getName());
      sendMetricsDelete(url);
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

  private Iterable<ProgramSpecification> getProgramSpecs(Id.Application appId) {
    ApplicationSpecification appSpec = store.getApplication(appId);
    Iterable<ProgramSpecification> programSpecs = Iterables.concat(appSpec.getFlows().values(),
                                                                   appSpec.getMapReduce().values(),
                                                                   appSpec.getProcedures().values(),
                                                                   appSpec.getWorkflows().values());
    return programSpecs;
  }

  /**
   * Delete the jar location of the program.
   *
   * @param appId        applicationId.
   * @throws IOException if there are errors with location IO
   */
  private void deleteProgramLocations(Id.Application appId) throws IOException {
    Iterable<ProgramSpecification> programSpecs = getProgramSpecs(appId);
    String appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR);
    for (ProgramSpecification spec : programSpecs) {
      ProgramType type = ProgramTypes.fromSpecification(spec);
      Id.Program programId = Id.Program.from(appId, type, spec.getName());
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
      Id.Program programId = Id.Program.from(appId.getNamespaceId(), appId.getId(),
                                             ProgramType.WEBAPP, ProgramType.WEBAPP.name().toLowerCase());
      Location location = Programs.programLocation(locationFactory, appFabricDir, programId, ProgramType.WEBAPP);
      location.delete();
    } catch (FileNotFoundException e) {
      // expected exception when webapp is not present.
    }
  }

  /**
   * Delete stored Preferences of the application and all its programs.
   * @param appId applicationId
   */
  private void deletePreferences(Id.Application appId) {
    Iterable<ProgramSpecification> programSpecs = getProgramSpecs(appId);
    for (ProgramSpecification spec : programSpecs) {

      preferencesStore.deleteProperties(appId.getNamespaceId(), appId.getId(),
                                        ProgramTypes.fromSpecification(spec).getCategoryName(), spec.getName());
      LOG.trace("Deleted Preferences of Program : {}, {}, {}, {}", appId.getNamespaceId(), appId.getId(),
                ProgramTypes.fromSpecification(spec).getCategoryName(), spec.getName());
    }
    preferencesStore.deleteProperties(appId.getNamespaceId(), appId.getId());
    LOG.trace("Deleted Preferences of Application : {}, {}", appId.getNamespaceId(), appId.getId());
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
        if (programState == ProgramController.State.COMPLETED || programState == ProgramController.State.ERROR
          || programState == ProgramController.State.KILLED) {
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

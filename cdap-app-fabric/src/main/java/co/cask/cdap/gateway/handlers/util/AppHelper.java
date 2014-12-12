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

package co.cask.cdap.gateway.handlers.util;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.services.DeployStatus;
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
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.SessionInfo;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.filesystem.LocationCodec;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.ning.http.client.Body;
import com.ning.http.client.BodyGenerator;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.commons.io.IOUtils;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Helper class to perform application-related tasks for {@link co.cask.http.HttpHandler} classes in the gateway
 *
 * TODO: Eventually, none of the methods in this class should need the {@link HttpRequest} parameter.
 * TODO: Also eventually, the accountId parameter in these methods should be replaced with namespaceId
 */
public class AppHelper {

  private static final Logger LOG = LoggerFactory.getLogger(AppHelper.class);

  private static final Gson GSON = new Gson();

  private static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";

  /**
   * Timeout to upload to remote app fabric.
   */
  private static final long UPLOAD_TIMEOUT = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

  /**
   * Number of seconds for timing out a service endpoint discovery.
   */
  private static final long DISCOVERY_TIMEOUT_SECONDS = 3;

  /**
   * Timeout to get response from metrics system.
   */
  private static final long METRICS_SERVER_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

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
   * Configuration object passed from higher up.
   */
  private final CConfiguration configuration;

  /**
   * Factory for handling the location - can do both in either Distributed or Local mode.
   */
  private final LocationFactory locationFactory;

  private final Scheduler scheduler;

  private final ProgramHelper programHelper;

  private final DiscoveryServiceClient discoveryServiceClient;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final StreamConsumerFactory streamConsumerFactory;

  private final QueueAdmin queueAdmin;

  /**
   * Maintains a mapping of transient session state. The state is stored in memory,
   * in case of failure, all the current running sessions will be terminated. As
   * per the current implementation only connection per account is allowed to upload.
   */
  private final Map<String, SessionInfo> sessions = Maps.newConcurrentMap();

  @Inject
  public AppHelper(ManagerFactory<Location, ApplicationWithPrograms> managerFactory,
                   CConfiguration configuration, LocationFactory locationFactory, Scheduler scheduler,
                   ProgramHelper programHelper, StoreFactory storeFactory,
                   StreamConsumerFactory streamConsumerFactory, QueueAdmin queueAdmin,
                   DiscoveryServiceClient discoveryServiceClient) {
    this.managerFactory = managerFactory;
    this.configuration = configuration;
    this.appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
    this.archiveDir = appFabricDir + "/archive";
    this.locationFactory = locationFactory;
    this.scheduler = scheduler;
    this.programHelper = programHelper;
    this.store = storeFactory.create();
    this.streamConsumerFactory = streamConsumerFactory;
    this.queueAdmin = queueAdmin;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  public BodyConsumer deployApp(HttpRequest request, HttpResponder responder, String accountId, String appId) {
    try {
      return deployAppStream(request, responder, accountId, appId);
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Deploy failed: {}" + ex.getMessage());
      return null;
    }
  }

  private BodyConsumer deployAppStream (final HttpRequest request, final HttpResponder responder,
                                        final String accountId, final String appId) throws IOException {
    final String archiveName = request.getHeader(ARCHIVE_NAME_HEADER);

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

    final Location archiveDir = locationFactory.create(this.archiveDir).append(accountId);
    final Location archive = archiveDir.append(archiveName);

    final SessionInfo sessionInfo = new SessionInfo(accountId, appId, archiveName, archive, DeployStatus.UPLOADING);
    sessions.put(accountId, sessionInfo);

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

          sessionInfo.setStatus(DeployStatus.VERIFYING);
          deploy(accountId, appId, archive);
          sessionInfo.setStatus(DeployStatus.DEPLOYED);
          responder.sendString(HttpResponseStatus.OK, "Deploy Complete");
        } catch (Exception e) {
          sessionInfo.setStatus(DeployStatus.FAILED);
          LOG.error("Deploy failure", e);
          responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        } finally {
          save(sessionInfo.setStatus(sessionInfo.getStatus()), accountId);
          sessions.remove(accountId);
        }
      }
    };
  }

  // deploy helper
  private void deploy(final String accountId, final String appId , Location archive) throws Exception {

    try {
      Id.Account id = Id.Account.from(accountId);
      Location archiveLocation = archive;
      Manager<Location, ApplicationWithPrograms> manager = managerFactory.create(new ProgramTerminator() {
        @Override
        public void stop(Id.Account id, Id.Program programId, ProgramType type) throws ExecutionException {
          programHelper.deleteHandler(programId, type);
        }
      });

      ApplicationWithPrograms applicationWithPrograms =
        manager.deploy(id, appId, archiveLocation).get();
      ApplicationSpecification specification = applicationWithPrograms.getSpecification();
      setupSchedules(accountId, specification);
    } catch (Throwable e) {
      LOG.warn(e.getMessage(), e);
      throw new Exception(e.getMessage());
    }
  }

  public void getAppDetails(HttpResponder responder, String accountId, String appid) {
    if (appid != null && appid.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "app-id is empty");
      return;
    }

    try {
      Id.Account accId = Id.Account.from(accountId);
      List<ApplicationRecord> result = Lists.newArrayList();
      List<ApplicationSpecification> specList;
      if (appid == null) {
        specList = new ArrayList<ApplicationSpecification>(store.getAllApplications(accId));
      } else {
        ApplicationSpecification appSpec = store.getApplication(new Id.Application(accId, appid));
        if (appSpec == null) {
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          return;
        }
        specList = Collections.singletonList(store.getApplication(new Id.Application(accId, appid)));
      }

      for (ApplicationSpecification appSpec : specList) {
        result.add(makeAppRecord(appSpec));
      }

      String json;
      if (appid == null) {
        json = GSON.toJson(result);
      } else {
        json = GSON.toJson(result.get(0));
      }

      responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                              ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception : ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Saves the {@link SessionInfo} to the filesystem.
   *
   * @param info to be saved.
   * @return true if and only if successful; false otherwise.
   */
  private boolean save(SessionInfo info, String accountId) {
    try {
      Gson gson = new GsonBuilder().registerTypeAdapter(Location.class, new LocationCodec(locationFactory)).create();
      Location outputDir = locationFactory.create(archiveDir).append(accountId);
      if (!outputDir.exists()) {
        return false;
      }
      final Location sessionInfoFile = outputDir.append("session.json");
      OutputSupplier<Writer> writer = new OutputSupplier<Writer>() {
        @Override
        public Writer getOutput() throws IOException {
          return new OutputStreamWriter(sessionInfoFile.getOutputStream(), "UTF-8");
        }
      };

      Writer w = writer.getOutput();
      try {
        gson.toJson(info, w);
      } finally {
        Closeables.closeQuietly(w);
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      return false;
    }
    return true;
  }

  private void setupSchedules(String accountId, ApplicationSpecification specification) throws IOException {

    for (Map.Entry<String, WorkflowSpecification> entry : specification.getWorkflows().entrySet()) {
      Id.Program programId = Id.Program.from(accountId, specification.getName(), entry.getKey());
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

  /**
   * Defines the class for sending deploy status to client.
   */
  private static class Status {
    private final int code;
    private final String status;
    private final String message;

    public Status(int code, String message) {
      this.code = code;
      this.status = DeployStatus.getMessage(code);
      this.message = message;
    }
  }

  public void getDeployStatus(HttpResponder responder, String accountId) {
    try {
      DeployStatus status  = dstatus(accountId);
      LOG.trace("Deployment status call at AppFabricHttpHandler , Status: {}", status);
      responder.sendJson(HttpResponseStatus.OK, new Status(status.getCode(), status.getMessage()));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /*
   * Returns DeploymentStatus
   */
  private DeployStatus dstatus(String accountId) {
    if (!sessions.containsKey(accountId)) {
      SessionInfo info = retrieve(accountId);
      if (info == null) {
        return DeployStatus.NOT_FOUND;
      }
      return info.getStatus();
    } else {
      SessionInfo info = sessions.get(accountId);
      return info.getStatus();
    }
  }

  /*
    * Retrieves a {@link SessionInfo} from the file system.
    */
  @Nullable
  private SessionInfo retrieve(String accountId) {
    try {
      final Location outputDir = locationFactory.create(archiveDir).append(accountId);
      if (!outputDir.exists()) {
        return null;
      }
      final Location sessionInfoFile = outputDir.append("session.json");
      InputSupplier<Reader> reader = new InputSupplier<Reader>() {
        @Override
        public Reader getInput() throws IOException {
          return new InputStreamReader(sessionInfoFile.getInputStream(), "UTF-8");
        }
      };

      Gson gson = new GsonBuilder().registerTypeAdapter(Location.class, new LocationCodec(locationFactory)).create();
      Reader r = reader.getInput();
      try {
        return gson.fromJson(r, SessionInfo.class);
      } finally {
        Closeables.closeQuietly(r);
      }
    } catch (IOException e) {
      LOG.warn("Failed to retrieve session info for account.");
    }
    return null;
  }

  public void deleteApp(HttpResponder responder, String accountId, String appId) {
    try {
      Id.Program id = Id.Program.from(accountId, appId, "");
      AppFabricServiceStatus appStatus = removeApplication(id);
      LOG.trace("Delete call for Application {} at AppFabricHttpHandler", appId);
      responder.sendString(appStatus.getCode(), appStatus.getMessage());
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public void deleteAllApps(HttpResponder responder, String accountId) {
    try {
      Id.Account id = Id.Account.from(accountId);
      AppFabricServiceStatus status = removeAll(id);
      LOG.trace("Delete All call at AppFabricHttpHandler");
      responder.sendString(status.getCode(), status.getMessage());
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private AppFabricServiceStatus removeAll(Id.Account identifier) throws Exception {
    List<ApplicationSpecification> allSpecs = new ArrayList<ApplicationSpecification>(
      store.getAllApplications(identifier));

    //Check if any App associated with this account is running
    final Id.Account accId = Id.Account.from(identifier.getId());
    boolean appRunning = programHelper.checkAnyRunning(new Predicate<Id.Program>() {
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
    boolean appRunning = programHelper.checkAnyRunning(new Predicate<Id.Program>() {
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

  public void promoteApp(HttpRequest request, HttpResponder responder, String accountId, String appId) {
    try {
      String postBody = null;

      try {
        postBody = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));
      } catch (IOException e) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }

      Map<String, String> content;
      try {
        content = GSON.fromJson(postBody, Constants.STRING_MAP_TYPE);
      } catch (JsonSyntaxException e) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST, "Not a valid body specified.");
        return;
      }

      if (!content.containsKey("hostname")) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST, "Hostname not specified.");
        return;
      }

      // Checks DNS, Ipv4, Ipv6 address in one go.
      String hostname = content.get("hostname");
      Preconditions.checkArgument(!hostname.isEmpty(), "Empty hostname passed.");

      String token = request.getHeader(Constants.Gateway.API_KEY);

      final Location appArchive = store.getApplicationArchiveLocation(Id.Application.from(accountId, appId));
      if (appArchive == null || !appArchive.exists()) {
        throw new IOException("Unable to locate the application.");
      }

      if (!promote(token, accountId, appId, hostname)) {
        responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to promote application " + appId);
      } else {
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public void deleteMetrics(String accountId, String applicationId) throws IOException, OperationException {
    Collection<ApplicationSpecification> applications = Lists.newArrayList();
    if (applicationId == null) {
      applications = this.store.getAllApplications(new Id.Account(accountId));
    } else {
      ApplicationSpecification spec = this.store.getApplication
        (new Id.Application(new Id.Account(accountId), applicationId));
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

  private boolean promote(String authToken, String accountId, String appId, String hostname) throws Exception {

    try {
      final Location appArchive = store.getApplicationArchiveLocation(Id.Application.from(accountId,
                                                                                          appId));
      if (appArchive == null || !appArchive.exists()) {
        throw new Exception("Unable to locate the application.");
      }

      String schema = "https";
      if ("localhost".equals(hostname)) {
        schema = "http";
      }

      // Construct URL for promotion of application to remote cluster
      int gatewayPort;
      if (configuration.getBoolean(Constants.Security.SSL_ENABLED)) {
        gatewayPort = Integer.parseInt(configuration.get(Constants.Router.ROUTER_SSL_PORT,
                                                         Constants.Router.DEFAULT_ROUTER_SSL_PORT));
      } else {
        gatewayPort = Integer.parseInt(configuration.get(Constants.Router.ROUTER_PORT,
                                                         Constants.Router.DEFAULT_ROUTER_PORT));
      }

      String url = String.format("%s://%s:%s/v2/apps/%s",
                                 schema, hostname, gatewayPort, appId);

      SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
        .setUrl(url)
        .setRequestTimeoutInMs((int) UPLOAD_TIMEOUT)
        .setHeader("X-Archive-Name", appArchive.getName())
        .setHeader(Constants.Gateway.API_KEY, authToken)
        .build();

      try {
        Future<Response> future = client.put(new LocationBodyGenerator(appArchive));
        Response response = future.get(UPLOAD_TIMEOUT, TimeUnit.MILLISECONDS);
        if (response.getStatusCode() != 200) {
          throw new RuntimeException(response.getResponseBody());
        }
        return true;
      } finally {
        client.close();
      }
    } catch (Exception ex) {
      LOG.warn(ex.getMessage(), ex);
      throw ex;
    }
  }

  private static final class LocationBodyGenerator implements BodyGenerator {

    private final Location location;

    private LocationBodyGenerator(Location location) {
      this.location = location;
    }

    @Override
    public Body createBody() throws IOException {
      final InputStream input = location.getInputStream();

      return new Body() {
        @Override
        public long getContentLength() {
          try {
            return location.length();
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public long read(ByteBuffer buffer) throws IOException {
          // Fast path
          if (buffer.hasArray()) {
            int len = input.read(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            if (len > 0) {
              buffer.position(buffer.position() + len);
            }
            return len;
          }

          byte[] bytes = new byte[buffer.remaining()];
          int len = input.read(bytes);
          if (len < 0) {
            return len;
          }
          buffer.put(bytes, 0, len);
          return len;
        }

        @Override
        public void close() throws IOException {
          input.close();
        }
      };
    }
  }

  private static ApplicationRecord makeAppRecord(ApplicationSpecification appSpec) {
    return new ApplicationRecord("App", appSpec.getName(), appSpec.getName(), appSpec.getDescription());
  }
}

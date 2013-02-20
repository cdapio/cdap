/*
* Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
*/

package com.continuuity.internal.app.services;

import com.continuuity.app.Id;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeployStatus;
import com.continuuity.app.services.DeploymentService;
import com.continuuity.app.services.DeploymentServiceException;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.deploy.SessionInfo;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.filesystem.LocationCodec;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metrics2.frontend.MetricsFrontendServiceImpl;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This is a concrete implementation of DeploymentService thrift Interface.
 * Following are the highlights of this service.
 * <p>
 *   <ul>This service does not talk to zookeeper. The states are maintained on the filesystem.</ul>
 *   <ul>In case of server crash, any states maintained within the server in terms of upload sessions
 *   will be lost. The client anyway has to connect back to the server.</ul>
 * </p>
 */
public class DeploymentServer implements DeploymentService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(DeploymentServer.class);

  /**
   * Maintains a mapping of transient session state. The state is stored in memory,
   * in case of failure, all the current running sessions will be terminated. As
   * per the current implementation only connection per account is allowed to upload.
   */
  private final Map<String, SessionInfo> sessions = Maps.newConcurrentMap();

  /**
   * Metadata Service instance is used to interact with the metadata store.
   */
  private final MetadataService mds;

  /**
   * Instance of operation executor needed by MetadataService.
   */
  private final OperationExecutor opex;

  /**
   * Configuration object passed from higher up.
   */
  private final CConfiguration configuration;

  /**
   * Factory for handling the location - can do both in either Distributed or Local mode.
   */
  private final LocationFactory locationFactory;

  /**
   * DeploymentManager responsible for running pipeline.
   */
  private final ManagerFactory managerFactory;

  /**
   * Authorization Factory used to create handler used for authroizing use of endpoints.
   */
  private final AuthorizationFactory authFactory;

  /**
   * The directory where the uploaded files would be placed.
   */
  private final String archiveDir;

  /**
   * Construct this object with curator client for managing the zookeeper.
   */
  public DeploymentServer(CConfiguration configuration, OperationExecutor opex,
                          LocationFactory locationFactory, ManagerFactory managerFactory,
                          AuthorizationFactory authFactory) {
    this.opex = opex;
    this.locationFactory = locationFactory;
    this.configuration = configuration;
    this.managerFactory = managerFactory;
    this.authFactory = authFactory;
    this.archiveDir = configuration.get("app.output.dir", "/tmp") + "/archive";
    this.mds = new MetadataService(opex);
  }

  /**
   * Initializes deployment of resources from the client.
   * <p>
   *   Upon receiving a request to initialize an upload with auth-token and resource information,
   *   we create a unique identifier for the upload and also create directories needed for storing
   *   the uploading archive. At this point the upload has not yet begun. The bytes of the archive
   *   are still on the client machine. An session id is returned back to client - which will use
   *   the session id provided to upload the chunks.
   * </p>
   * <p>
   *   <i>Note:</i> As the state of upload are transient they are not being persisted on the server.
   * </p>
   *
   * @param info ResourceInfo
   * @return ResourceIdentifier instance containing the resource id and
   * resource version.
   */
  @Override
  public ResourceIdentifier init(AuthToken token, ResourceInfo info) throws DeploymentServiceException {
    ResourceIdentifier identifier = new ResourceIdentifier( info.getAccountId(), "", "", 1);

    try {
      if(sessions.containsKey(info.getAccountId())) {
        throw new DeploymentServiceException("An upload is already in progress for this account.");
      }
      Location uploadDir = locationFactory.create(archiveDir + "/" + info.getAccountId());
      if(! uploadDir.exists() && ! uploadDir.mkdirs()) {
        LOG.warn("Unable to create directory '{}'", uploadDir.getName());
      }
      Location archive = uploadDir.append(info.getFilename());
      SessionInfo sessionInfo = new SessionInfo(identifier, info, archive, DeployStatus.REGISTERED);
      sessions.put(info.getAccountId(), sessionInfo);
      return identifier;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Writes chunk of data transmitted from the client. Along with data, there is the session id also
   * being returned.
   *
   * @param resource identifier.
   * @param chunk binary data of the resource transmitted from the client.
   * @throws DeploymentServiceException
   */
  @Override
  public void chunk(AuthToken token, ResourceIdentifier resource, ByteBuffer chunk) throws DeploymentServiceException {
    if(! sessions.containsKey(resource.getAccountId())) {
      throw new DeploymentServiceException("A session id has not been created for upload. Please call #init");
    }

    SessionInfo info = sessions.get(resource.getAccountId()).setStatus(DeployStatus.UPLOADING);
    try {
      OutputStream stream = info.getOutputStream();
      // Read the chunk from ByteBuffer and write it to file
      if(chunk != null) {
        byte[] buffer = new byte[chunk.remaining()];
        chunk.get(buffer);
        stream.write(buffer);
      } else {
        sessions.remove(resource.getAccountId());
        throw new DeploymentServiceException("Invalid chunk received.");
      }
    } catch (IOException e) {
      sessions.remove(resource.getAccountId());
      throw new DeploymentServiceException("Failed to write archive chunk");
    }
  }

  /**
   * Finalizes the deployment of a archive. Once upload is completed, it will
   * start the pipeline responsible for verification and registration of archive resources.
   *
   * @param resource identifier to be finalized.
   */
  @Override
  public void deploy(AuthToken token, final ResourceIdentifier resource) throws DeploymentServiceException {
    if(!sessions.containsKey(resource.getAccountId())) {
      throw new DeploymentServiceException("No information about archive being uploaded is available.");
    }

    try {
      Id.Account id = Id.Account.from(resource.getAccountId());
      Location archiveLocation = sessions.get(resource.getAccountId()).getArchiveLocation();
      OutputStream stream = sessions.get(resource.getAccountId()).getOutputStream();
      try {
        sessions.get(resource.getAccountId()).setStatus(DeployStatus.VERIFYING);
        Manager<Location, ApplicationWithPrograms> manager
          = (Manager<Location, ApplicationWithPrograms>)managerFactory.create(configuration);
        ListenableFuture<ApplicationWithPrograms> future = manager.deploy(id, archiveLocation);
        Futures.addCallback(future, new FutureCallback<ApplicationWithPrograms>() {
          @Override
          public void onSuccess(ApplicationWithPrograms result) {
            save(sessions.get(resource.getAccountId()).setStatus(DeployStatus.DEPLOYED));
            sessions.remove(resource.getAccountId());
          }

          @Override
          public void onFailure(Throwable t) {
            save(sessions.get(resource.getAccountId()).setStatus(DeployStatus.FAILED));
            sessions.remove(resource.getAccountId());
          }
        });
      } finally {
        stream.close();
      }
    } catch (Throwable e) {
      save(sessions.get(resource.getAccountId()).setStatus(DeployStatus.FAILED));
      sessions.remove(resource.getAccountId());
      throw new DeploymentServiceException(e.getMessage());
    }
  }

  /**
   * Returns status of deployment of archive.
   *
   * @param resource identifier
   * @return status of resource processing.
   * @throws DeploymentServiceException
   */
  public DeploymentStatus status(AuthToken token, ResourceIdentifier resource) throws DeploymentServiceException {
    if(!sessions.containsKey(resource.getAccountId())) {
      SessionInfo info = retrieve(resource.getAccountId());
      DeploymentStatus status = new DeploymentStatus(info.getStatus().getCode(),
                                                     info.getStatus().getMessage(), null);
      return status;
    } else {
      SessionInfo info = sessions.get(resource.getAccountId());
      DeploymentStatus status = new DeploymentStatus(info.getStatus().getCode(),
                                                     info.getStatus().getMessage(), null);
      return status;
    }
  }

  /**
   * Promotes a FAR from single node to cloud.
   *
   * @param identifier of the flow.
   * @return true if successful; false otherwise.
   * @throws DeploymentServiceException
   */
  @Override
  public boolean promote(AuthToken authToken, ResourceIdentifier identifier) throws DeploymentServiceException {
    return false;
  }


  /**
   * Deletes a flow specified by {@code FlowIdentifier}.
   *
   * @param identifier of a flow.
   * @throws DeploymentServiceException when there is an issue deactivating the flow.
   */
  @Override
  public void remove(AuthToken token, FlowIdentifier identifier) throws DeploymentServiceException {
    Preconditions.checkNotNull(token);
  }

  @Override
  public void removeAll(AuthToken token, String account) throws DeploymentServiceException {
    Preconditions.checkNotNull(token);
  }

  @Override
  public void reset(AuthToken token, String account) throws DeploymentServiceException {
    Preconditions.checkNotNull(account);

    deleteMetrics(account);
    // delete all meta data
    try {
      mds.deleteAll(account);
    } catch (Exception e) {
      String message = String.format("Error deleting all meta data for " +
                                       "account '%s': %s. At %s", account, e.getMessage(),
                                     StackTraceUtil.toStringStackTrace(e));
      LOG.error(message);
      throw new DeploymentServiceException(message);
    }

    // wipe the data fabric
    try {
      LOG.info("Deleting all data for account '" + account + "'.");
      opex.execute(
                    new OperationContext(account),
                    new ClearFabric(ClearFabric.ToClear.ALL)
      );
      LOG.info("All data for account '" + account + "' deleted.");
    } catch (Exception e) {
      String message = String.format("Error deleting the data for " +
                                       "account '%s': %s. At %s", account, e.getMessage(),
                                     StackTraceUtil.toStringStackTrace(e));
      LOG.error(message);
      throw new DeploymentServiceException(message);
    }
  }

  /**
   * Deletes metrics for a given account.
   *
   * @param account for which the metrics need to be reset.
   * @throws DeploymentServiceException throw due to issue in reseting metrics for
   * account.
   */
  private void deleteMetrics(String account) throws DeploymentServiceException {
    try {
      LOG.info("Deleting all metrics for account '" + account + "'.");
      MetricsFrontendServiceImpl mfs =
        new MetricsFrontendServiceImpl(configuration);
      mfs.reset(account);
      LOG.info("All metrics for account '" + account + "'deleted.");
    } catch (Exception e) {
      String message = String.format("Error clearing the metrics for " +
                                       "account '%s': %s. At %s", account, e.getMessage(),
                                     StackTraceUtil.toStringStackTrace(e));
      LOG.error(message);
      throw new DeploymentServiceException(message);
    }
  }

  /**
   * Saves the {@link SessionInfo} to the filesystem.
   *
   * @param info to be saved.
   * @return true if and only if successful; false otherwise.
   */
  private boolean save(SessionInfo info) {
    try {
      Gson gson = new GsonBuilder().registerTypeAdapter(Location.class, new LocationCodec(locationFactory)).create();
      String accountId = info.getResourceIdenitifier().getAccountId();
      Location outputDir = locationFactory.create(archiveDir + "/" + accountId);
      if(! outputDir.exists()) {
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
      LOG.warn("Failed to write session.");
      return false;
    }
    return true;
  }

  /**
   * Retrieves a {@link SessionInfo} from the file system.
   *
   * @param accountId to which the
   * @return
   */
  @Nullable
  private SessionInfo retrieve(String accountId) {
    try {
      final Location outputDir = locationFactory.create(archiveDir + "/" + accountId);
      if(! outputDir.exists()) {
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
        SessionInfo info = gson.fromJson(r, SessionInfo.class);
        return info;
      } finally {
        Closeables.closeQuietly(r);
      }
    } catch (IOException e) {
      LOG.warn("Failed to retrieve session info for account.");
    }
    return null;
  }
}

/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.services.AppFabricClient;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeployableStatus;
import com.continuuity.app.services.DeploymentService;
import com.continuuity.app.services.DeploymentServiceException;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.deploy.DeployableManager;
import com.continuuity.internal.app.deploy.DeployableRegInfo;
import com.continuuity.internal.app.deploy.DeployableStorage;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metrics2.frontend.MetricsFrontendServiceImpl;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

public class DeploymentServiceImpl implements DeploymentService.Iface {
  private static final Logger LOG
    = LoggerFactory.getLogger(DeploymentServiceImpl.class);

  /**
   * Manages registration of resources.
   */
  private final DeployableManager deployableManager;

  /**
   * Provides the storage handler for Flow Manager.
   */
  private final DeployableStorage storage;

  /**
   * Keeps a cache of ResourceIdentifier to DeployableRegInfo mapping
   * to reduce hits to ZK.
   */
  private final Map<ResourceIdentifier, DeployableRegInfo>
    cache = Maps.newConcurrentMap();

  /**
   * Keeps the mapping of currently active writes.
   */
  private final Map<ResourceIdentifier, OutputStream>
    activeDeploys = Maps.newConcurrentMap();

  /**
   * Instance of configuration.
   */
  private final CConfiguration conf;

  /**
   * meta data service to register streams
   */
  private final MetadataService mds;

  /**
   * the operation executor
   */
  private final OperationExecutor opex;

  /**
   * Construct this object with curator client for managing the zookeeper.
   * @param client The remote Client to use.
   */
  @Inject
  public DeploymentServiceImpl(CuratorFramework client, DeployableStorage storage,
                           OperationExecutor opex, CConfiguration conf) {
    this.storage = storage;
    this.conf = conf;
    this.opex = opex;
    this.deployableManager = new DeployableManager(client);
    this.mds = new MetadataService(opex);
  }
  
  /**
   * Registers the resource with zookeeper.
   *
   * <p>
   *   Creates a new unique identifier (UUID) and associates that with the
   *   resource. A node is created in ZK under
   *   <code>FLOW_SERVICE_RESOURCE_PATH</code>
   *   with resource identifier as one of the nodes. Under that node information
   *   about the FAR is stored. Also, the status of the FAR is stored.
   * </p>
   *
   * <p>
   *   NOTE: In future, the <code>ResourceInfo</code> could contain version
   *   info too.
   * </p>
   *
   * @param info ResourceInfo
   * @return ResourceIdentifier instance containing the resource id and
   * resource version.
   */
  public ResourceIdentifier init(AuthToken token, ResourceInfo info)
    throws DeploymentServiceException {

    Preconditions.checkNotNull(token);

    LOG.debug("Registering resource {}", info.toString());

    // Create a unique resource identifier.
    String id = UUID.nameUUIDFromBytes(info.getFilename().getBytes()).toString();

    // Get the next available version for the resource with this id
    int version = deployableManager.getNextAvailableVersion(id);

    ResourceIdentifier identifier = new ResourceIdentifier(
                                                            info.getAccountId(),
                                                            info.getApplicationId(),
                                                            id, version);

    if (version == -1) {
      throw new DeploymentServiceException("Unable to get the next available " +
                                      "version for the resource");
    }

    Location jar;
    try {
      jar = storage.getNewLocation(info.getFilename(), identifier);
    } catch (IOException e) {
      LOG.warn("Storage failed to create a new storage space for " +
                 "resource {}. Reason: {}", new Object[]{info.toString(),
                                                          e.getMessage()});
      throw new DeploymentServiceException("Failed to register resource " +
                                      "with resource manager. Resource : " + info.toString());
    }

    // Registers the resource with id and version
    DeployableRegInfo regInfo = new DeployableRegInfo(identifier, info, jar);
    if (! deployableManager.register(regInfo)) {
      LOG.warn("Failed to register resource with resource manager. " +
                 "Resource info : " + info.toString());
      throw new DeploymentServiceException("Failed to register resource with " +
                                      "resource manager. Resource info : " + info.toString());
    }
    cache.put(identifier, regInfo);
    return identifier;
  }


  /**
   * Writes the chunk of data transmitted from the client to the preconfigured
   * file for a resource.
   * @param resource identifier.
   * @param chunk binary data of the resource transmitted from the client.
   * @throws DeploymentServiceException
   */
  public void chunk(AuthToken token, ResourceIdentifier resource, ByteBuffer chunk)
    throws DeploymentServiceException {
    DeployableRegInfo regInfo;
    if(! cache.containsKey(resource)) {
      regInfo = deployableManager.getDeployableRegInfo(resource);
    } else {
      regInfo = cache.get(resource);
    }

    try {
      OutputStream stream;
      if(! activeDeploys.containsKey(resource)) {
        deployableManager.setDeployableStatus(resource,
                                              DeployableStatus.newStatus((DeployableStatus.UPLOADING)));

        stream = regInfo.getJarLocation().getOutputStream();
        activeDeploys.put(resource, stream);
      } else {
        stream = activeDeploys.get(resource);
      }

      // Read the chunk from ByteBuffer and write it to file
      if(chunk != null) {
        byte[] buffer = new byte[chunk.remaining()];
        chunk.get(buffer);
        stream.write(buffer);
      } else {
        throw new DeploymentServiceException("Received null chunk. Please " +
                                        "make sure you are sending binary data correctly.");
      }
    } catch (IOException e) {
      LOG.warn("Failed writing chunk for resource {}. " + "Reason: {}",
               new Object[]{resource, e.getMessage()});
      throw new DeploymentServiceException("Failed to write the chunk to " +
                                      "file system for resource " + resource.toString());
    }
  }

  /**
   * Finalizes the deployment of a resource. Once upload is completed, it will
   * start the verification step where it tries to verify the deployed resource.
   *
   * @param resource identifier to be finalized.
   */
  public void deploy(AuthToken token, final ResourceIdentifier resource)
    throws DeploymentServiceException {

    if(! activeDeploys.containsKey(resource)) {
      LOG.error("Resource is currently not in the active list for " +
                  "uploading jar. Inconsistent state");
      throw new DeploymentServiceException(String.format("Resource %s in " +
                                                           "consistent state", resource.toString()));
    }

    // close the stream
    OutputStream stream = activeDeploys.get(resource);
    try {
      stream.close();
    } catch (IOException e) {
      LOG.warn("Issue while closing stream for resource {}",
               resource.toString());
    }

    // TODO: start the program deploy pipeline

    // Once it's been uploaded and verification has started, you can remove
    // the resource identifier from the
    // active list.
    activeDeploys.remove(resource);
    deployableManager.setDeployableStatus(resource, DeployableStatus.newStatus(DeployableStatus.VERIFYING));

    // TODO: hack: setting status to DEPLOYED (should be done in the pipeline above)
    deployableManager.setDeployableStatus(resource,
                                          DeployableStatus.newStatus(DeployableStatus.DEPLOYED));
  }

  /**
   * Promotes a FAR from single node to cloud.
   *
   * @param identifier of the flow.
   * @return true if successful; false otherwise.
   * @throws DeploymentServiceException
   */
  public boolean promote(AuthToken authToken, ResourceIdentifier identifier) throws DeploymentServiceException {
    Preconditions.checkNotNull(authToken);
    Preconditions.checkNotNull(identifier);

    // Retrieve the registration information associated with the flow.
    DeployableRegInfo regInfo = deployableManager.getDeployableRegInfo(identifier);
    if(regInfo == null) {
      throw new DeploymentServiceException("Unable to find resource information " +
                                             "for the flow.");
    }

    // Get the path for the jar associated with the flow.
    Location srcPath = regInfo.getJarLocation();
    if(srcPath == null) {
      throw new DeploymentServiceException("No resource was recorded for this flow.");
    }

    // Construct a local path name for it to be uploaded.
    String localBasePath = conf.get(Constants.CFG_RESOURCE_MANAGER_LOCAL_DIR,
                                    Constants.DEFAULT_RESOURCE_MANAGER_LOCAL_DIR);
    File localCopyPath = new File(localBasePath + "/" + regInfo.getFilename());

    // Create a filesystem handler. In distributed mode, the jar is placed on DFS
    // and needs to be copied to local disk before it can be uploaded.
    try {
      FileUtils.copyInputStreamToFile(srcPath.getInputStream(), localCopyPath);
    } catch (IOException e) {
      throw new DeploymentServiceException(e.getMessage());
    }

    // Read the host and port configuration from the configuration object.
    String remoteHost = conf.get(Constants.CFG_RESOURCE_MANAGER_CLOUD_HOST,
                                 Constants.DEFAULT_RESOURCE_MANAGER_CLOUD_HOST);
    int remotePort = conf.getInt(Constants.CFG_RESOURCE_MANAGER_CLOUD_PORT,
                                 Constants.DEFAULT_RESOURCE_MANAGER_CLOUD_PORT);

    LOG.info("Uploading the flow in file {} to remote host {}:{}",
             localCopyPath.toString(), remoteHost, remotePort);

    AppFabricClient localClient;
    try {
      // Connect to server on remote host.
      try {
        localClient = new AppFabricClient(conf, true);
      } catch (Exception e) {
        return false;
      }

      // Deploy the file to cloud.
      localCopyPath.deleteOnExit(); // delete the file on exit.

      ImmutablePair<Boolean, String> result = localClient.deploy(localCopyPath,
                                                                 identifier.getAccountId(),
                                                                 identifier.getApplicationId());
      LOG.info("Deployed resource {} to cloud, Status : {}, hostname : {}, port : {}",
               localCopyPath.toString(), result.getSecond(), remoteHost, remotePort);

      return result.getFirst();
    } catch (IOException e) {
      LOG.info("Failed deploying resource {} to cloud. Reason : {}",
               localCopyPath.toString(), e.getMessage());
      throw new DeploymentServiceException(e.getMessage());
    }
  }

  /**
   * Returns status of processing identified by resource.
   * @param resource identifier
   * @return status of resource processing.
   * @throws DeploymentServiceException
   */
  public DeploymentStatus status(AuthToken token, ResourceIdentifier resource)
    throws DeploymentServiceException {
    DeployableStatus status = deployableManager.getDeployableStatus(resource);
    return new DeploymentStatus(status.getCode(), status.getMessage(), null);
  }

  /**
   * Deletes a flow specified by {@code FlowIdentifier}.
   *
   * @param identifier of a flow.
   * @throws DeploymentServiceException when there is an issue deactivating the flow.
   */
  public void remove(AuthToken token, FlowIdentifier identifier) throws DeploymentServiceException {
    Preconditions.checkNotNull(token);

    // TODO
  }

  public void removeAll(AuthToken token, String account) throws DeploymentServiceException {
    Preconditions.checkNotNull(token);

    // TODO
  }

  public void reset(AuthToken token, String account) throws DeploymentServiceException {
    Preconditions.checkNotNull(account);

    LOG.info("Wiping out account '" + account + "'.");

    // delete all metrics data for account
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
        new MetricsFrontendServiceImpl(conf);
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
}

package com.continuuity.internal.app.deploy;

import com.continuuity.app.services.DeployableStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.serializer.JSONSerializer;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class is responsible for managing FAR resource lifecycle.
 */
public final class DeployableManager {
  private static final Logger Log = LoggerFactory.getLogger(DeployableManager.class);

  private final CuratorFramework client;

  /**
   * Creates an instance of Resource Manager using the ZK curator client.
   * @param client reference to zookeeper client.
   */
  public DeployableManager(CuratorFramework client) {
    this.client = client;
  }

  /**
   * Registers a resource with zookeeper.
   *
   * @param data associated with the version.
   * @return version of the resource.
   */
  public boolean register(DeployableRegInfo data) {
    ResourceIdentifier id = data.getResourceIdenitifier();
    JSONSerializer<DeployableRegInfo> rrSerializer = new JSONSerializer<DeployableRegInfo>();
    JSONSerializer<DeployableStatus> rsSerializer = new JSONSerializer<DeployableStatus>();

    String infoPath =
      String.format(DeployableZookeeperPaths.VERSIONED_RESOURCE_INFO, id.getResource(), id.getVersion());
    String statusPath =
      String.format(DeployableZookeeperPaths.VERSIONED_RESOURCE_STATUS, id.getResource(), id.getVersion());

    try {
      DeployableStatus status = DeployableStatus.newStatus(DeployableStatus.REGISTERED);
      byte[] info = rrSerializer.serialize(data);
      client.inTransaction()
        .create().withMode(CreateMode.PERSISTENT).forPath(infoPath, info)
        .and()
        .create().withMode(CreateMode.PERSISTENT).forPath(statusPath, rsSerializer.serialize(status))
        .and()
        .commit();
    } catch (Exception e) {
      Log.error("Unable to register resource " + data.toString() + " on path "
        + getVersionPath(id.getResource(), id.getVersion()));
      return false;
    }
    return true;
  }


  /**
   * Returns the zookeeper path for a resource.
   * @param id of the resource
   * @return path to node in zk for resource with id.
   */
  private String getResourcePath(String id) {
    return DeployableZookeeperPaths.construct(DeployableZookeeperPaths.VERSIONED_RESOURCE_BASE, id);
  }

  /**
   * Returns the path to a version of a resource with id.
   * @param id of the resource.
   * @param version version of resource.
   * @return Path including version and resource.
   */
  private String getVersionPath(String id, int version) {
    return String.format("%s/%d", getResourcePath(id), version);
  }

  /**
   * Gets the next available version number for a give resource id.
   *
   * @param id of resource to be versioned.
   * @return next available version number for the given resource id.
   */
  public synchronized int getNextAvailableVersion(String id) {
    String resourcePath = getResourcePath(id);
    int version = -1;

    boolean findAvailableVersion = true;
    int attempts = 0;
    while(findAvailableVersion) {
      try {
        if(client.checkExists().forPath(resourcePath) == null) {
          EnsurePath rpath = new EnsurePath(resourcePath);
          rpath.ensure(client.getZookeeperClient());
        }
        version = client.checkExists().forPath(resourcePath).getNumChildren();
        version = version + 1;
        client.create().withMode(CreateMode.PERSISTENT).forPath(getVersionPath(id, version));
        int afterVersion = client.checkExists().forPath(resourcePath).getNumChildren();
        if(afterVersion == version) {
          findAvailableVersion = false;
        }
      } catch (Exception e) {
        Log.error("Could not acquire version. Attempt number " + attempts);
        attempts++;
      }

      if(! findAvailableVersion) {
        break;
      }

      if(attempts > Constants.RESOURCE_MANAGER_VERSION_FIND_ATTEMPTS) {
        break;
      }
    }
    return version;
  }

  /**
   * Gets the status of a resource with id <code>id</code> and version <code>version</code>
   *
   * @param identifier of the resource
   * @return {@link DeployableStatus} instance.
   */
  public DeployableStatus getDeployableStatus(ResourceIdentifier identifier) {
    DeployableStatus status;
    String statusPath = String.format(DeployableZookeeperPaths.VERSIONED_RESOURCE_STATUS,
      identifier.getResource(), identifier.getVersion());
    try {
      byte[] data = client.getData().forPath(statusPath);
      JSONSerializer<DeployableStatus> statusSerializer
        = new JSONSerializer<DeployableStatus>();
      status = statusSerializer.deserialize(data, DeployableStatus.class);
    } catch (Exception e) {
      Log.warn("Resource {}, version {} not found under path {}",
        new Object[] { identifier.getResource(), identifier.getVersion(), statusPath });
      status = DeployableStatus.newStatus(DeployableStatus.NOT_FOUND);
    }
    return status;
  }

  /**
   * Sets the stats of a resource with a version to <code>status</code>
   *
   * @param identifier  of the resource
   * @param status status to be set.
   * @return true if successful; false otherwise.
   */
  public boolean setDeployableStatus(ResourceIdentifier identifier, DeployableStatus status) {
    String statusPath = String.format(DeployableZookeeperPaths.VERSIONED_RESOURCE_STATUS,
      identifier.getResource(), identifier.getVersion());
    try {
      JSONSerializer<DeployableStatus> statusSerializer
        = new JSONSerializer<DeployableStatus>();
      byte[] data = statusSerializer.serialize(status);
      client.setData().forPath(statusPath, data);
      return true;
    } catch (Exception e) {
      Log.info("Unable to change status of resource {}, version {} to {}",
        new Object[] { identifier.getResource(), identifier.getVersion(), status});
    }
    return false;
  }

  /**
   * Gets the registration information about a resource identifier by the ResourceIdentifier.
   *
   * @param id of the resource for which we need to retrieve the registration information.
   * @return {@link DeployableRegInfo} instance containing resource info; null otherwise.
   */
  public DeployableRegInfo getDeployableRegInfo(ResourceIdentifier id) {
    DeployableRegInfo regInfo = null;
    String infoPath =
      String.format(DeployableZookeeperPaths.VERSIONED_RESOURCE_INFO, id.getResource(), id.getVersion());
    try {
      byte[] data = client.getData().forPath(infoPath);
      JSONSerializer<DeployableRegInfo> regInfoJSONSerializer = new JSONSerializer<DeployableRegInfo>();
      regInfo = regInfoJSONSerializer.deserialize(data, DeployableRegInfo.class);
    } catch (Exception e) {
      Log.info("Failed to retrieve registration info for resource {}", id);
    }
    return regInfo;
  }


  public void unregisterFlow(String accountId, String app, String flow, int version) {
    String path =
      String.format(DeployableZookeeperPaths.APPLICATION_FLOW_VERSIONED_BASE, accountId, app, flow, version);
    try {
      client.delete().guaranteed().forPath(path);
    } catch (Exception e) {
      Log.warn("Failed to unregister app {}, please delete the path {} manually.", app, path);
    }
  }
}

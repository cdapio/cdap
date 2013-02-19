package com.continuuity.internal.app.deploy;

/**
 * Zookeeper paths used across all of the flow systems are specified here.
 */
public final class DeployableZookeeperPaths {

  /**
   * Utility class, hence not allowing construction of object.
   */
  private DeployableZookeeperPaths() {}

  /**
   * Registers the UUID of resource under this path. %s here specifies the
   * unique identifier for a resource being uploaded. Node data contains a
   * json object with information like when it was registered, name of resource,
   * user who did it.
   */
  public static final String VERSIONED_RESOURCE_BASE
    = "/continuuity/system/resources/%s";

  /**
   * Resource modifications are managed as version. For each modification
   * a version number is assigned automagically.
   */
  public static final String VERSIONED_RESOURCE_INFO
    = VERSIONED_RESOURCE_BASE + "/%d/info";

  /**
   * Overall status of the resource being deployed.
   */
  public static final String VERSIONED_RESOURCE_STATUS
    = VERSIONED_RESOURCE_BASE + "/%d/status";

  /**
   * ZK path for an application without the version.
   * URI : {account}/{application}/{flow}
   */
  public static final String APPLICATION_FLOW_BASE
    = "/continuuity/apps/%s/%s/%s";

  /**
   * ZK path for an application with the version.
   * URI: {account}/{application}/{flow}/{version}
   */
  public static final String APPLICATION_FLOW_VERSIONED_BASE
    = "/continuuity/apps/%s/%s/%s/%d";

  /**
   * Path to Flowlet in ZK tree.
   * URI: {account}/{application}/{flow}/{version}/flowlets/{flowlet}
   */
  public static final String APPLICATION_FLOWLET
    = "/continuuity/apps/%s/%s/%s/%d/flowlets/%s";

  /**
   * Path to Instances of flowlets in ZK tree.
   * URI : {account}/{application}/{flow}/{version}/flowlets/{flowlet}/instances
   */
  public static final String APPLICATION_FLOWLET_INSTANCES
    = "/continuuity/apps/%s/%s/%s/%d/flowlets/%s/instances";

  /**
   * Constructs a ZK path.
   * @param format A format string.
   * @param args Arguments referenced by the format specifiers in the format string. If there are more
   *             arguments than format specifiers, the extra arguments are ignored. The number of arguments
   *             is variable and may be zero.
   * @return A formatted string.
   */
  public static String construct(String format, Object... args) {
    return String.format(format, args);
  }

}

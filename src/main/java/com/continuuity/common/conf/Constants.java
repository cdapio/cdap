package com.continuuity.common.conf;

/**
 * Constants used by different systems are all defined here.
 */
public class Constants {

  /**
   * Common across components.
   */
  public static final String CFG_ZOOKEEPER_ENSEMBLE = "zookeeper.quorum";

  /**
   * Default constants for common
   */
  public static final String DEFAULT_ZOOKEEPER_ENSEMBLE = "localhost:2181";

  /**
   * Configuration key names used by resource manager.
   */
  public static final String CFG_RESOURCE_MANAGER_REMOTE_DIR = "resource.manager.remote.dir";
  public static final String CFG_RESOURCE_MANAGER_LOCAL_DIR = "resource.manager.local.dir";
  public static final String CFG_RESOURCE_MANAGER_STORAGE_MODE = "resource.manager.storage.mode";
  public static final String CFG_RESOURCE_MANAGER_SERVER_PORT = "resource.manager.server.port";
  public static final String CFG_RESOURCE_MANAGER_SERVER_THREADS = "resource.manager.server.threads";

  /**
   * Default constants defined for resource manager
   */
  public static final String DEFAULT_RESOURCE_MANAGER_SERVER_PORT = "45000";
  public static final String DEFAULT_RESOURCE_MANAGER_SERVER_THREADS = "2";

  /**
   * Configuration key names used flow monitor.
   */
  public static final String CFG_FLOW_MONITOR_SERVER_PORT = "flow.monitor.server.port";
  public static final String CFG_FLOW_MONITOR_SERVER_THREADS = "flow.monitor.server.threads";

  /**
   * Default constants defined for flow monitor
   */
  public static final String DEFAULT_FLOW_MONITOR_SERVER_PORT = "45002";
  public static final String DEFAULT_FLOW_MONITOR_SERVER_THREADS = "2";
}

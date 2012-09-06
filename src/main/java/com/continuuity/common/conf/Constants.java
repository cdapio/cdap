package com.continuuity.common.conf;

/**
 * Constants used by different systems are all defined here.
 */
public class Constants {

  /**
   * Service names.
   */
  public static final String
    SERVICE_METRICS_COLLECTION_SERVER = "metricscollection";

  public static final String
    SERVICE_METRICS_FRONTEND_SERVER = "metricsfrontend";

  /**
   * Common across components.
   */
  public static final String CFG_ZOOKEEPER_ENSEMBLE = "zookeeper.quorum";

  // ENG-443 setting for avoiding OOME when telnetd to server port.
  public static final String CFG_MAX_READ_BUFFER = "thrift.max.read.buffer";

  // ENG-440 Connection URL for location used for storing the flows states
  // and history.
  public static final String CFG_STATE_STORAGE_CONNECTION_URL =
    "state.storage.connection.url";

  public static final String CFG_COMMAND_PORT_ENABLED =
    "command.port.enabled";

  /**
   * Default constants for common
   */
  public static final String DEFAULT_ZOOKEEPER_ENSEMBLE = "localhost:2181";

  // ENG-443 1 MB max read buffer size.
  public static final int DEFAULT_MAX_READ_BUFFER = 1048576;

  // ENG-440 Connection URL for location used for storing the flows states
  // and history.
  public static final String DEFAULT_STATE_STORAGE_CONNECTION_URL =
    "jdbc:hsqldb:mem:flowmanagement?user=sa";

  public static final boolean DEFAULT_COMMAND_PORT_ENABLED = false;

  /**
   * Configuration key names used by resource manager.
   */
  public static final String
    CFG_RESOURCE_MANAGER_REMOTE_DIR = "resource.manager.remote.dir";
  public static final String
    CFG_RESOURCE_MANAGER_LOCAL_DIR = "resource.manager.local.dir";
  public static final String
    CFG_RESOURCE_MANAGER_SERVER_PORT = "resource.manager.server.port";
  public static final String
    CFG_RESOURCE_MANAGER_SERVER_THREADS = "resource.manager.server.threads";
  public static final String
    CFG_RESOURCE_MANAGER_SERVER_ADDRESS = "resource.manager.server.address";

  /**
   * Defaults for resource manager.
   */
  public static final String
    DEFAULT_RESOURCE_MANAGER_LOCAL_DIR = "build/continuuity/flow/manager/local";
  public static final String
    DEFAULT_RESOURCE_MANAGER_REMOTE_DIR = "build/continuuity/flow/manager/remote";
  public static final String
    DEFAULT_RESOURCE_MANAGER_SERVER_ADDRESS = "0.0.0.0";

  /**
   * Constants used by resource manager.
   */
  public static final String JAR_EXTENSION=".jar";
  public static final String STANDARD_FAR_FLOWS_CONFIG_FILE = "flows.json";
  public static final String MANIFEST_FILE_PATH = "META-INF/MANIFEST.MF";
  public static final int RESOURCE_MANAGER_VERSION_FIND_ATTEMPTS = 10;

  /**
   * Default constants defined for resource manager
   */
  public static final String DEFAULT_RESOURCE_MANAGER_SERVER_PORT = "45000";
  public static final String DEFAULT_RESOURCE_MANAGER_SERVER_THREADS = "2";


  /**
   * Constants used by Tuple serializer
   */
  public static final int MAX_SERDE_BUFFER = 1024 * 1024;

  /**
   * Configuration key names used by flow manager
   */
  public static final String CFG_FLOW_MANAGER_SERVER_PORT =
        "flow.manager.server.port";
  public static final String CFG_FLOW_MANAGER_SERVER_THREADS =
        "flow.manager.server.threads";
  public static final String CFG_FLOW_MANAGER_SERVER_ADDRESS =
        "flow.manager.server.address";


  /**
   * Default constants defined for flow manager.
   */
  public static final String DEFAULT_FLOW_MANAGER_SERVER_PORT = "45001";
  public static final String DEFAULT_FLOW_MANAGER_SERVER_THREADS = "2";
  public static final String DEFAULT_FLOW_MANAGER_SERVER_ADDRESS = "0.0.0.0";


  /**
   * Configuration for Cloud FAR Service.
   */
  public static final String
    CFG_RESOURCE_MANAGER_CLOUD_HOST = "resource.manager.cloud.hostname";
  public static final String
    CFG_RESOURCE_MANAGER_CLOUD_PORT = "resource.manager.cloud.port";
  public static final String
    DEFAULT_RESOURCE_MANAGER_CLOUD_HOST = "localhost";
  public static final String
    DEFAULT_RESOURCE_MANAGER_CLOUD_PORT = DEFAULT_RESOURCE_MANAGER_SERVER_PORT;

  /**
   * Configuration for OpenTSDB
   */
  public static final String
    CFG_OPENTSDB_SERVER_ADDRESS = "opentsdb.server.address";
  public static final String
    CFG_OPENTSDB_SERVER_PORT = "opentsdb.server.port";

  /**
   * Defaults for OpenTSDB
   */
  public static final String DEFAULT_OPENTSDB_SERVER_ADDRESS = "localhost";
  public static final int DEFAULT_OPENTSDB_SERVER_PORT = 4242;

  /**
   * Configuration for Metrics collection server
   */
  public static final String
    CFG_METRICS_COLLECTOR_SERVER_ADDRESS
    = "overlord.metrics.collection.server.address";
  public static final String
    CFG_METRICS_COLLECTOR_SERVER_PORT
    = "overlord.metrics.collection.server.port";
  public static final String
    CFG_METRICS_COLLECTION_SERVER_OPENTSDB_ENABLED
    = "ovelord.metrics.server.opentsdb.enabled";
  public static final String
    CFG_METRICS_FRONTEND_SERVER_ADDRESS
    = "overlord.metrics.frontend.server.address";
  public static final String
    CFG_METRICS_FRONTEND_SERVER_PORT
    = "overlord.metrics.frontend.server.port";
  public static final String
    CFG_METRICS_FRONTEND_SERVER_THREADS
    = "overlord.metrics.frontend.server.threads";
  public static final String
    CFG_METRICS_CONNECTION_URL = "overlord.metrics.connection.url";

  /**
   * Defaults for metrics collection server
   */
  public static final String
    DEFAULT_METRICS_COLLECTOR_SERVER_ADDRESS = "localhost";
  public static final int DEFAULT_METRICS_COLLECTOR_SERVER_PORT = 45003;
  public static final boolean
    DEFAULT_METRICS_COLLECTION_SERVER_OPENTSDB_ENABLED = false;
  public static final String
    DEFAULT_METRICS_FRONTEND_SERVER_ADDRESS = "localhost";
  public static final int
    DEFAULT_METRICS_FRONTEND_SERVER_PORT = 45002;
  public static final int
    DEFAULT_METRICS_FRONTEND_SERVER_THREADS = 2;
  public static final String
    DEFAULT_METIRCS_CONNECTION_URL = "jdbc:hsqldb:mem:metricsdb?user=sa";
}

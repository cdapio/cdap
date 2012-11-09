package com.continuuity.common.conf;

/**
 * Constants used by different systems are all defined here.
 */
public class Constants {

  /**
   * The default account id to use
   */
  public static String DEFAULT_ACCOUNT_ID = "demo";

  /**
   * Service names.
   */
  public static final String
    SERVICE_METRICS_COLLECTION_SERVER = "metricscollection";
  public static final String
    SERVICE_METRICS_FRONTEND_SERVER = "metricsfrontend";
  public static final String
      SERVICE_METADATA_SERVER = "metadata";
  public static final String
      SERVICE_FLOW_SERVER = "flow-service";

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
  public static final int DEFAULT_RESOURCE_MANAGER_SERVER_PORT = 45000;
  public static final int DEFAULT_RESOURCE_MANAGER_SERVER_THREADS = 2;


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
  public static final int DEFAULT_FLOW_MANAGER_SERVER_PORT = 45001;
  public static final int DEFAULT_FLOW_MANAGER_SERVER_THREADS = 2;
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
  public static final int
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
  public static final String
    CFG_METRICS_COLLECTION_FLOW_SYSTEM_PLUGINS
    = "overlord.metrics.processor.plugins.flowsystem";
  public static final String
    CFG_METRICS_COLLECTION_SYSTEM_PLUGINS
    = "overlord.metrics.processor.plugins.system";
  public static final String
    CFG_METRICS_COLLECTION_FLOW_USER_PLUGINS
    = "overlord.metrics.processor.plugins.flowuser";
  public static final String
    CFG_METRICS_COLLECTION_ALLOWED_TIMESERIES_METRICS
    = "overlord.metrics.timeseries.metrics";

  /**
   * Defaults for metrics collection server
   */
  public static final String
    DEFAULT_METRICS_COLLECTOR_SERVER_ADDRESS = "localhost";
  public static final int DEFAULT_METRICS_COLLECTOR_SERVER_PORT = 45003;
  public static final String
    DEFAULT_METRICS_FRONTEND_SERVER_ADDRESS = "localhost";
  public static final int
    DEFAULT_METRICS_FRONTEND_SERVER_PORT = 45002;
  public static final int
    DEFAULT_METRICS_FRONTEND_SERVER_THREADS = 2;
  public static final String
    DEFAULT_METIRCS_CONNECTION_URL = "jdbc:hsqldb:mem:metricsdb?user=sa";
  public static final String
    DEFAULT_METRICS_COLLECTION_ALLOWED_TIMESERIES_METRICS = "processed.count";

  /**
   * Configuration for Metadata service
   */
  public static final String
    CFG_METADATA_SERVER_ADDRESS = "metadata.server.address";
  public static final String
    CFG_METADATA_SERVER_PORT = "metadata.server.port";
  public static final String
    CFG_METADATA_SERVER_THREADS = "metadata.server.threads";

  /**
   * Defaults for metadata service
   */
  public static final String
    DEFAULT_METADATA_SERVER_ADDRESS = "localhost";
  public static final int
    DEFAULT_METADATA_SERVER_PORT = 45004;
  public static final int
    DEFAULT_METADATA_SERVER_THREADS = 2;

  /**
   * Configuration for consolidated overlord service.
   */
  public static final String
    CFG_OVERLORD_SERVER_ADDRESS = "overlord.server.address";
  public static final String
    CFG_OVERLORD_SERVER_PORT = "overlord.server.port";
  public static final String
    CFG_OVERLORD_SERVER_THREADS = "overlord.server.threads";

  /**
   * Defaults for Overlord service
   */
  public static final String
    DEFAULT_OVERLORD_SERVER_ADDRESS = "localhost";
  public static final int
    DEFAULT_OVERLORD_SERVER_PORT = 45005;
  public static final int
    DEFAULT_OVERLORD_SERVER_THREADS = 10;

  /**
   * Config for Log Collection
   */
  public static final String CFG_LOG_COLLECTION_ROOT =
      "log.collection.root";
  public static final String DEFAULT_LOG_COLLECTION_ROOT =
      "/continuuity/data/logs";

}

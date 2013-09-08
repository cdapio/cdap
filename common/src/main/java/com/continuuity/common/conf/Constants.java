package com.continuuity.common.conf;

/**
 * Constants used by different systems are all defined here.
 */
public class Constants {
  /**
   * Service names.
   */
  public static final String SERVICE_METRICS_COLLECTION_SERVER = "metricscollection";
  public static final String SERVICE_METRICS_FRONTEND_SERVER = "metricsfrontend";
  public static final String SERVICE_METADATA_SERVER = "metadata";
  public static final String SERVICE_TRANSACTION_SERVER = "tx-service";
  public static final String SERVICE_METRICS = "metricsservice";

  /**
   * Common across components.
   */
  public static final String CFG_ZOOKEEPER_ENSEMBLE = "zookeeper.quorum";
  public static final String CFG_MAX_READ_BUFFER = "thrift.max.read.buffer";
  public static final String CFG_LOCAL_DATA_DIR = "local.data.dir";
  public static final String CFG_YARN_USER = "yarn.user";
  public static final String CFG_HDFS_USER = "hdfs.user";
  public static final String CFG_HDFS_NAMESPACE = "hdfs.namespace";
  public static final String CFG_WEAVE_ZK_NAMESPACE = "weave.zookeeper.namespace";

  /**
   * Default constants for common.
   */
  public static final String DEFAULT_ZOOKEEPER_ENSEMBLE = "localhost:2181";
  public static final int DEFAULT_MAX_READ_BUFFER = 16 * 1024 * 1024;
  public static final int DEFAULT_APP_FABRIC_SERVER_PORT = 45000;
  public static final String DEFAULT_APP_FABRIC_SERVER_ADDRESS = "localhost";


  /**
   * App Fabric Server.
   */
  public static final String CFG_APP_FABRIC_SERVER_ADDRESS = "app.server.address";
  public static final String CFG_APP_FABRIC_SERVER_PORT = "app.server.port";
  public static final String CFG_APP_FABRIC_OUTPUT_DIR  = "app.output.dir";
  public static final String CFG_APP_FABRIC_TEMP_DIR    = "app.temp.dir";
  public static final String CFG_APP_FABRIC_REST_PORT   = "app.rest.port";

  /**
   * Data Fabric.
   */
  public static enum InMemoryPersistenceType {
    MEMORY,
    LEVELDB,
    HSQLDB
  }
  /** defines which persistence engine to use when running all in one JVM. **/
  public static final String CFG_DATA_INMEMORY_PERSISTENCE = "data.local.inmemory.persistence.type";
  public static final String CFG_DATA_LEVELDB_ENABLED = "data.local.storage.enabled";
  public static final String CFG_DATA_LEVELDB_DIR = "data.local.storage";
  public static final String CFG_DATA_LEVELDB_BLOCKSIZE = "data.local.storage.blocksize";
  public static final String CFG_DATA_LEVELDB_CACHESIZE = "data.local.storage.cachesize";
  public static final String CFG_DATA_LEVELDB_FSYNC = "data.local.storage.fsync";

  /** Minimum count of table write ops executed by opex to try to apply batching logic to. */
  public static final String CFG_DATA_TABLE_WRITE_OPS_BATCH_MIN_SIZE = "data.table.ops.batch.min";
  /** Max puts to perform in one rpc. */
  public static final String CFG_DATA_HBASE_PUTS_BATCH_MAX_SIZE = "data.dist.hbase.put.batch_size.max";
  /** Max threads to use to write into single HBase table. */
  public static final String CFG_QUEUE_STATE_PROXY_MAX_CACHE_SIZE_BYTES = "queue.state.proxy.max.cache.size.bytes";
  public static final String CFG_DATA_HBASE_TABLE_WRITE_THREADS_MAX_COUNT =
    "data.dist.hbase.table.write_threads_count.max";


  /**
   * Defaults for Data Fabric.
   */
  public static final String DEFAULT_DATA_INMEMORY_PERSISTENCE = InMemoryPersistenceType.MEMORY.name();
  public static final String DEFAULT_DATA_LEVELDB_DIR = "data";
  public static final int DEFAULT_DATA_LEVELDB_BLOCKSIZE = 1024;
  public static final long DEFAULT_DATA_LEVELDB_CACHESIZE = 1024 * 1024 * 100;
  public static final boolean DEFAULT_DATA_LEVELDB_FSYNC = true;

  /** I.e. by default do NOT attempt to batch table write ops. */
  public static final int DEFAULT_DATA_TABLE_WRITE_OPS_BATCH_MIN_SIZE = Integer.MAX_VALUE;
  /** I.e. by default do NOT limit puts count per rpc. */
  public static final int DEFAULT_DATA_HBASE_PUTS_BATCH_MAX_SIZE = Integer.MAX_VALUE;
  /** Use 10 threads per table by default. */
  public static final int DEFAULT_DATA_HBASE_TABLE_WRITE_THREADS_MAX_COUNT = 10;
  public static final long DEAFULT_CFG_QUEUE_STATE_PROXY_MAX_CACHE_SIZE_BYTES = 200 * 1024 * 1024;

 /**
   * Configuration for Metrics collection server.
   */
  public static final String CFG_METRICS_FRONTEND_SERVER_ADDRESS = "overlord.metrics.frontend.server.address";
  public static final String CFG_METRICS_FRONTEND_SERVER_PORT = "overlord.metrics.frontend.server.port";
  public static final String CFG_METRICS_FRONTEND_SERVER_THREADS = "overlord.metrics.frontend.server.threads";

  /**
   * Defaults for metrics collection server.
   */
  public static final int DEFAULT_METRICS_FRONTEND_SERVER_PORT = 45002;
  public static final int DEFAULT_METRICS_FRONTEND_SERVER_THREADS = 2;

  /**
   * Configuration for Metadata service.
   */
  public static final String CFG_METADATA_SERVER_ADDRESS = "metadata.server.address";
  public static final String CFG_METADATA_SERVER_PORT = "metadata.server.port";
  public static final String CFG_METADATA_SERVER_THREADS = "metadata.server.threads";

  /**
   * Defaults for metadata service.
   */
  public static final int DEFAULT_METADATA_SERVER_PORT = 45004;
  public static final int DEFAULT_METADATA_SERVER_THREADS = 2;

  /**
   * Config for Log Collection.
   */
  public static final String CFG_LOG_COLLECTION_ROOT = "log.collection.root";
  public static final String DEFAULT_LOG_COLLECTION_ROOT = "data/logs";
  public static final String CFG_LOG_COLLECTION_PORT = "log.collection.port";
  public static final int DEFAULT_LOG_COLLECTION_PORT = 12157;
  public static final String CFG_LOG_COLLECTION_THREADS = "log.collection.threads";
  public static final int DEFAULT_LOG_COLLECTION_THREADS = 10;
  public static final String CFG_LOG_COLLECTION_SERVER_ADDRESS = "log.collection.server.address";
  public static final String DEFAULT_LOG_COLLECTION_SERVER_ADDRESS = "localhost";

  /**
   * Constants related to Passport.
   */
  public static final String CFG_APPFABRIC_ENVIRONMENT = "appfabric.environment";
  public static final String DEFAULT_APPFABRIC_ENVIRONMENT = "devsuite";


  /**
   * Corresponds to account id used when running in local mode.
   * NOTE: value should be in sync with the one used by UI.
   */
  public static final String DEVELOPER_ACCOUNT_ID = "developer";
}

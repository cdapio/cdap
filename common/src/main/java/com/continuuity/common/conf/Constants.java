package com.continuuity.common.conf;

/**
 * Constants used by different systems are all defined here.
 */
public final class Constants {
  /**
   * Global Service names.
   */
  public static final class Service {
    public static final String APP_FABRIC = "app.fabric";
    public static final String METRICS_FRONTEND = "metrics.frontend";
    public static final String METADATA = "metadata";
    public static final String TRANSACTION = "transaction";
    public static final String METRICS = "metrics";
  }

  /**
   * Zookeeper Configuration.
   */
  public static final class Zookeeper {
    public static final String QUORUM = "zookeeper.quorum";
    public static final String DEFAULT_ZOOKEEPER_ENSEMBLE = "localhost:2181";
  }

  /**
   * Thrift configuration.
   */
  public static final class Thrift {
    public static final String MAX_READ_BUFFER = "thrift.max.read.buffer";
    public static final int DEFAULT_MAX_READ_BUFFER = 16 * 1024 * 1024;
  }

  /**
   * Dangerous Options
   */
  public static final class Dangerous {
    public static final String UNRECOVERABLE_RESET = "enable.unrecoverable.reset";
    public static final boolean DEFAULT_UNRECOVERABLE_RESET = false;
  }

  /**
   * App Fabric Configuration
   */
  public static final class AppFabric {
    /**
     * Default constants for common.
     */
    public static final int DEFAULT_SERVER_PORT = 45000;
    public static final String DEFAULT_SERVER_ADDRESS = "localhost";


    /**
     * App Fabric Server.
     */
    public static final String SERVER_ADDRESS = "app.server.address";
    public static final String SERVER_PORT = "app.server.port";
    public static final String OUTPUT_DIR = "app.output.dir";
    public static final String TEMP_DIR = "app.temp.dir";
    public static final String REST_PORT = "app.rest.port";
  }

  /**
   * Gateway Configurations
   */
  public static final class Gateway {
    public static final String ADDRESS = "gateway.server.address";
    public static final String PORT = "stream.rest.port";
    public static final String BACKLOG = "gateway.connection.backlog";
    public static final String EXEC_THREADS = "gateway.exec.threads";
    public static final String BOSS_THREADS = "gateway.boss.threads";
    public static final String WORKER_THREADS = "gateway.worker.threads";
    public static final String MAX_CACHED_STREAM_EVENTS_NUM = "gateway.max.cached.stream.events.num";
    public static final String MAX_CACHED_EVENTS_PER_STREAM_NUM = "gateway.max.cached.events.per.stream.num";
    public static final String MAX_CACHED_STREAM_EVENTS_BYTES = "gateway.max.cached.stream.events.bytes";
    public static final String STREAM_EVENTS_FLUSH_INTERVAL_MS = "gateway.stream.events.flush.interval.ms";
    public static final String STREAM_EVENTS_CALLBACK_NUM_THREADS = "gateway.stream.callback.exec.num.threads";
    public static final String CONFIG_AUTHENTICATION_REQUIRED = "authentication.required";
    public static final String CLUSTER_NAME = "cluster.name";

    /**
     * Defaults
     */
    public static final int DEFAULT_PORT = 10000;
    public static final int DEFAULT_BACKLOG = 20000;
    public static final int DEFAULT_EXEC_THREADS = 20;
    public static final int DEFAULT_BOSS_THREADS = 1;
    public static final int DEFAULT_WORKER_THREADS = 10;
    public static final int DEFAULT_MAX_CACHED_STREAM_EVENTS_NUM = 10000;
    public static final int DEFAULT_MAX_CACHED_EVENTS_PER_STREAM_NUM = 5000;
    public static final long DEFAULT_MAX_CACHED_STREAM_EVENTS_BYTES = 50 * 1024 * 1024;
    public static final long DEFAULT_STREAM_EVENTS_FLUSH_INTERVAL_MS = 150;
    public static final int DEFAULT_STREAM_EVENTS_CALLBACK_NUM_THREADS = 5;
    public static final boolean CONFIG_AUTHENTICATION_REQUIRED_DEFAULT = false;
    public static final String CLUSTER_NAME_DEFAULT = "localhost";


    /**
     * Others
     */
    public static final String CONTINUUITY_PREFIX = "X-Continuuity-";
    public static final String GATEWAY_PREFIX = "gateway.";
    public static final String GATEWAY_V2_HTTP_HANDLERS = "gateway.http.handler";
    public static final String STREAM_HANDLER_NAME = "stream.rest";
    public static final String HEADER_STREAM_CONSUMER = "X-Continuuity-ConsumerId";
    public static final String HEADER_DESTINATION_STREAM = "X-Continuuity-Destination";
    public static final String HEADER_FROM_COLLECTOR = "X-Continuuity-FromCollector";
  }

  /**
   * Common across components.
   */
  public static final String CFG_ZOOKEEPER_ENSEMBLE = "zookeeper.quorum";
  public static final String CFG_LOCAL_DATA_DIR = "local.data.dir";
  public static final String CFG_YARN_USER = "yarn.user";
  public static final String CFG_HDFS_USER = "hdfs.user";
  public static final String CFG_HDFS_NAMESPACE = "hdfs.namespace";
  public static final String CFG_WEAVE_ZK_NAMESPACE = "weave.zookeeper.namespace";


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

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
    public static final String METADATA = "metadata";
    public static final String TRANSACTION = "transaction";
    public static final String METRICS = "metrics";
    public static final String GATEWAY = "gateway";
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
   * Dangerous Options.
   */
  public static final class Dangerous {
    public static final String UNRECOVERABLE_RESET = "enable.unrecoverable.reset";
    public static final boolean DEFAULT_UNRECOVERABLE_RESET = false;
  }

  /**
   * App Fabric Configuration.
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
    public static final String SERVER_ADDRESS = "app.bind.address";
    public static final String SERVER_PORT = "app.bind.port";
    public static final String OUTPUT_DIR = "app.output.dir";
    public static final String TEMP_DIR = "app.temp.dir";
    public static final String REST_PORT = "app.rest.port";
  }

  /**
   * Gateway Configurations.
   */
  public static final class Gateway {
    public static final String ADDRESS = "gateway.bind.address";
    public static final String PORT = "gateway.bind.port";
    public static final String BACKLOG_CONNECTIONS = "gateway.connection.backlog";
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
    public static final String NUM_CORES = "gateway.num.cores";
    public static final String NUM_INSTANCES = "gateway.num.instances";
    public static final String MEMORY_MB = "gateway.memory.mb";

    /**
     * Defaults.
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
    public static final int DEFAULT_NUM_CORES = 2;
    public static final int DEFAULT_NUM_INSTANCES = 1;
    public static final int DEFAULT_MEMORY_MB = 2048;


    /**
     * Others.
     */
    public static final String CONTINUUITY_PREFIX = "X-Continuuity-";
    public static final String GATEWAY_PREFIX = "gateway.";
    public static final String STREAM_HANDLER_NAME = "stream.rest";
    public static final String HEADER_STREAM_CONSUMER = "X-Continuuity-ConsumerId";
    public static final String HEADER_DESTINATION_STREAM = "X-Continuuity-Destination";
    public static final String HEADER_FROM_COLLECTOR = "X-Continuuity-FromCollector";
  }

  /**
   * Router Configuration.
   */
  public static final class Router {
    public static final String ADDRESS = "router.bind.address";
    public static final String PORT = "router.bind.port";
    public static final String DEST_SERVICE_NAME = "router.dest.service.name";
    public static final String BACKLOG_CONNECTIONS = "router.connection.backlog";
    public static final String SERVER_BOSS_THREADS = "router.server.boss.threads";
    public static final String SERVER_WORKER_THREADS = "router.server.worker.threads";
    public static final String CLIENT_BOSS_THREADS = "router.client.boss.threads";
    public static final String CLIENT_WORKER_THREADS = "router.client.worker.threads";

    /**
     * Defaults.
     */
    public static final int DEFAULT_PORT = 10000;
    public static final String DEFAULT_DEST_SERVICE_NAME = Service.GATEWAY;
    public static final int DEFAULT_BACKLOG = 20000;
    public static final int DEFAULT_SERVER_BOSS_THREADS = 1;
    public static final int DEFAULT_SERVER_WORKER_THREADS = 10;
    public static final int DEFAULT_CLIENT_BOSS_THREADS = 1;
    public static final int DEFAULT_CLIENT_WORKER_THREADS = 10;
  }

  /**
   * TransactionManager configuration.
   */
  public static final class TransactionManager {

    public static final String CFG_DO_PERSIST = "tx.persist";
    public static final String CFG_TX_SNAPSHOT_DIR = "data.tx.snapshot.dir";
    // how often to clean up timed out transactions, in seconds, or 0 for no cleanup
    public static final String CFG_TX_CLEANUP_INTERVAL = "data.tx.cleanup.interval";
    // how often to clean up timed out transactions, in seconds
    public static final int DEFAULT_TX_CLEANUP_INTERVAL = 60;
    // the timeout for a transaction, in seconds. If the transaction is not finished in that time, it is marked invalid
    public static final String CFG_TX_TIMEOUT = "data.tx.timeout";
    public static final int DEFAULT_TX_TIMEOUT = 300;
    // the frequency (in seconds) to perform periodic snapshots
    public static final String CFG_TX_SNAPSHOT_INTERVAL = "data.tx.snapshot.interval";
    public static final long DEFAULT_TX_SNAPSHOT_INTERVAL = 300;
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

  /**
   * Defaults for Data Fabric.
   */
  public static final String DEFAULT_DATA_INMEMORY_PERSISTENCE = InMemoryPersistenceType.MEMORY.name();
  public static final String DEFAULT_DATA_LEVELDB_DIR = "data";
  public static final int DEFAULT_DATA_LEVELDB_BLOCKSIZE = 1024;
  public static final long DEFAULT_DATA_LEVELDB_CACHESIZE = 1024 * 1024 * 100;
  public static final boolean DEFAULT_DATA_LEVELDB_FSYNC = true;

  /**
   * Configuration for Metadata service.
   */
  public static final String CFG_METADATA_SERVER_ADDRESS = "metadata.bind.address";
  public static final String CFG_METADATA_SERVER_PORT = "metadata.bind.port";
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
  public static final String CFG_LOG_COLLECTION_PORT = "log.collection.bind.port";
  public static final int DEFAULT_LOG_COLLECTION_PORT = 12157;
  public static final String CFG_LOG_COLLECTION_THREADS = "log.collection.threads";
  public static final int DEFAULT_LOG_COLLECTION_THREADS = 10;
  public static final String CFG_LOG_COLLECTION_SERVER_ADDRESS = "log.collection.bind.address";
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

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
    public static final String APP_FABRIC_LEADER_ELECTION_PREFIX = "election/appfabric";
  }

  /**
   * Zookeeper Configuration.
   */
  public static final class Zookeeper {
    public static final String QUORUM = "zookeeper.quorum";
    public static final String DEFAULT_ZOOKEEPER_ENSEMBLE = "localhost:2181";
    public static final String CFG_SESSION_TIMEOUT_MILLIS = "zookeeper.session.timeout.millis";
    public static final int DEFAULT_SESSION_TIMEOUT_MILLIS = 40000;
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
    public static final String SERVER_COMMAND_PORT = "app.command.port";
    public static final String OUTPUT_DIR = "app.output.dir";
    public static final String TEMP_DIR = "app.temp.dir";
    public static final String REST_PORT = "app.rest.port";
    public static final String PROGRAM_JVM_OPTS = "app.program.jvm.opts";
  }

  /**
   * Scheduler options.
   */
  public class Scheduler {
    public static final String CFG_SCHEDULER_MAX_THREAD_POOL_SIZE = "scheduler.max.thread.pool.size";
    public static final int DEFAULT_THREAD_POOL_SIZE = 30;
  }

  /**
   * Transactions
   */
  public static final class Transaction {
    /**
     * TransactionManager configuration.
     */
    public static final class Manager {
      // TransactionManager configuration
      public static final String CFG_DO_PERSIST = "tx.persist";
      /** Directory in HDFS used for transaction snapshot and log storage. */
      public static final String CFG_TX_SNAPSHOT_DIR = "data.tx.snapshot.dir";
      /** Directory on the local filesystem used for transaction snapshot and log storage. */
      public static final String CFG_TX_SNAPSHOT_LOCAL_DIR = "data.tx.snapshot.local.dir";
      /** How often to clean up timed out transactions, in seconds, or 0 for no cleanup. */
      public static final String CFG_TX_CLEANUP_INTERVAL = "data.tx.cleanup.interval";
      /** Default value for how often to check in-progress transactions for expiration, in seconds. */
      public static final int DEFAULT_TX_CLEANUP_INTERVAL = 10;
      /**
       * The timeout for a transaction, in seconds. If the transaction is not finished in that time,
       * it is marked invalid.
       */
      public static final String CFG_TX_TIMEOUT = "data.tx.timeout";
      /** Default value for transaction timeout, in seconds. */
      public static final int DEFAULT_TX_TIMEOUT = 30;
      /** The frequency (in seconds) to perform periodic snapshots, or 0 for no periodic snapshots. */
      public static final String CFG_TX_SNAPSHOT_INTERVAL = "data.tx.snapshot.interval";
      /** Default value for frequency of periodic snapshots of transaction state. */
      public static final long DEFAULT_TX_SNAPSHOT_INTERVAL = 300;
      /** Number of most recent transaction snapshots to retain. */
      public static final String CFG_TX_SNAPSHOT_RETAIN = "data.tx.snapshot.retain";
      /** Default value for number of most recent snapshots to retain. */
      public static final int DEFAULT_TX_SNAPSHOT_RETAIN = 10;
    }

    /**
     * TransactionService configuration.
     */
    public static final class Service {
  
      /** for the port of the tx server. */
      public static final String CFG_DATA_TX_BIND_PORT
        = "data.tx.bind.port";
  
      /** for the address (hostname) of the tx server. */
      public static final String CFG_DATA_TX_BIND_ADDRESS
        = "data.tx.bind.address";

      /** for the address (hostname) of the tx server command port. */
      public static final String CFG_DATA_TX_COMMAND_PORT
        = "data.tx.command.port";

      /** the number of IO threads in the tx service. */
      public static final String CFG_DATA_TX_SERVER_IO_THREADS
        = "data.tx.server.io.threads";
  
      /** the number of handler threads in the tx service. */
      public static final String CFG_DATA_TX_SERVER_THREADS
        = "data.tx.server.threads";
  
      /** default tx service port. */
      public static final int DEFAULT_DATA_TX_BIND_PORT
        = 15165;
  
      /** default tx service address. */
      public static final String DEFAULT_DATA_TX_BIND_ADDRESS
        = "0.0.0.0";
  
      /** default number of handler IO threads in tx service. */
      public static final int DEFAULT_DATA_TX_SERVER_IO_THREADS
        = 2;
  
      /** default number of handler threads in tx service. */
      public static final int DEFAULT_DATA_TX_SERVER_THREADS
        = 20;
  
      // Configuration key names and defaults used by tx client.
  
      /** to specify the tx client socket timeout in ms. */
      public static final String CFG_DATA_TX_CLIENT_TIMEOUT
        = "data.tx.client.timeout";
  
      /** to specify the tx client socket timeout for long-running ops in ms. */
      public static final String CFG_DATA_TX_CLIENT_LONG_TIMEOUT
        = "data.tx.client.long.timeout";
  
      /** to specify the tx client provider strategy. */
      public static final String CFG_DATA_TX_CLIENT_PROVIDER
        = "data.tx.client.provider";
  
      /** to specify the number of threads for client provider "pool". */
      public static final String CFG_DATA_TX_CLIENT_COUNT
        = "data.tx.client.count";
  
      /** to specify the retry strategy for a failed thrift call. */
      public static final String CFG_DATA_TX_CLIENT_RETRY_STRATEGY
        = "data.tx.client.retry.strategy";
  
      /** to specify the number of times to retry a failed thrift call. */
      public static final String CFG_DATA_TX_CLIENT_ATTEMPTS
        = "data.tx.client.retry.attempts";
  
      /** to specify the initial sleep time for retry strategy backoff. */
      public static final String CFG_DATA_TX_CLIENT_BACKOFF_INIITIAL
        = "data.tx.client.retry.backoff.initial";
  
      /** to specify the backoff factor for retry strategy backoff. */
      public static final String CFG_DATA_TX_CLIENT_BACKOFF_FACTOR
        = "data.tx.client.retry.backoff.factor";
  
      /** to specify the sleep time limit for retry strategy backoff. */
      public static final String CFG_DATA_TX_CLIENT_BACKOFF_LIMIT
        = "data.tx.client.retry.backoff.limit";
  
      /** the default tx client socket timeout in milli seconds. */
      public static final int DEFAULT_DATA_TX_CLIENT_TIMEOUT
        = 30 * 1000;
  
      /** tx client timeout for long operations such as ClearFabric. */
      public static final int DEFAULT_DATA_TX_CLIENT_LONG_TIMEOUT
        = 300 * 1000;
  
      /** default number of pooled tx clients. */
      public static final int DEFAULT_DATA_TX_CLIENT_COUNT
        = 5;
  
      /** default tx client provider strategy. */
      public static final String DEFAULT_DATA_TX_CLIENT_PROVIDER
        = "pool";
  
      /** retry strategy for thrift clients, e.g. backoff, or n-times. */
      public static final String DEFAULT_DATA_TX_CLIENT_RETRY_STRATEGY
        = "backoff";
  
      /** default number of attempts for strategy n-times. */
      public static final int DEFAULT_DATA_TX_CLIENT_ATTEMPTS
        = 2;
  
      /** default initial sleep is 100ms. */
      public static final int DEFAULT_DATA_TX_CLIENT_BACKOFF_INIITIAL
        = 100;
  
      /** default backoff factor is 4. */
      public static final int DEFAULT_DATA_TX_CLIENT_BACKOFF_FACTOR
        = 4;
  
      /** default sleep limit is 30 sec. */
      public static final int DEFAULT_DATA_TX_CLIENT_BACKOFF_LIMIT
        = 30 * 1000;
    }

    /**
     * Configuration for the TransactionDataJanitor coprocessor.
     */
    public static final class DataJanitor {
      /**
       * Whether or not the TransactionDataJanitor coprocessor should be enabled on tables.
       * Disable for testing.
       */
      public static final String CFG_TX_JANITOR_ENABLE = "data.tx.janitor.enable";
      public static final boolean DEFAULT_TX_JANITOR_ENABLE = true;
    }
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
    public static final String CONFIG_AUTHENTICATION_REQUIRED = "gateway.authenticate";
    public static final String CLUSTER_NAME = "gateway.cluster.name";
    public static final String NUM_CORES = "gateway.num.cores";
    public static final String NUM_INSTANCES = "gateway.num.instances";
    public static final String MEMORY_MB = "gateway.memory.mb";
    public static final String STREAM_FLUME_THREADS = "stream.flume.threads";
    public static final String STREAM_FLUME_PORT = "stream.flume.port";
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
    public static final int DEFAULT_STREAM_FLUME_THREADS = 10;
    public static final int DEFAULT_STREAM_FLUME_PORT = 10004;


    /**
     * Others.
     */
    public static final String GATEWAY_VERSION = "/v2";
    public static final String CONTINUUITY_PREFIX = "X-Continuuity-";
    public static final String STREAM_HANDLER_NAME = "stream.rest";
    public static final String METRICS_CONTEXT = "gateway." + Gateway.STREAM_HANDLER_NAME;
    public static final String FLUME_HANDLER_NAME = "stream.flume";
    public static final String HEADER_STREAM_CONSUMER = "X-Continuuity-ConsumerId";
    public static final String HEADER_DESTINATION_STREAM = "X-Continuuity-Destination";
    public static final String HEADER_FROM_COLLECTOR = "X-Continuuity-FromCollector";

    /**
     * query parameter to indicate start time
     */
    public static final String QUERY_PARAM_START_TIME = "before";

    /**
     * query parameter to indicate end time
     */
    public static final String QUERY_PARAM_END_TIME = "after";

    /**
     * Query parameter to indicate limits on results.
     */
    public static final String QUERY_PARAM_LIMIT = "limit";

    /**
     * Default history results limit.
     */
    public static final int DEFAULT_HISTORY_RESULTS_LIMIT = 100;

  }

  /**
   * Router Configuration.
   */
  public static final class Router {
    public static final String ADDRESS = "router.bind.address";
    public static final String FORWARD = "router.forward.rule";
    public static final String BACKLOG_CONNECTIONS = "router.connection.backlog";
    public static final String SERVER_BOSS_THREADS = "router.server.boss.threads";
    public static final String SERVER_WORKER_THREADS = "router.server.worker.threads";
    public static final String CLIENT_BOSS_THREADS = "router.client.boss.threads";
    public static final String CLIENT_WORKER_THREADS = "router.client.worker.threads";

    /**
     * Defaults.
     */
    public static final String DEFAULT_FORWARD = "10000:" + Service.GATEWAY;
    public static final int DEFAULT_BACKLOG = 20000;
    public static final int DEFAULT_SERVER_BOSS_THREADS = 1;
    public static final int DEFAULT_SERVER_WORKER_THREADS = 10;
    public static final int DEFAULT_CLIENT_BOSS_THREADS = 1;
    public static final int DEFAULT_CLIENT_WORKER_THREADS = 10;
  }

  /**
   * Webapp Configuration.
   */
  public static final class Webapp {
    public static final String WEBAPP_DIR = "webapp";
  }

  /**
   * Common across components.
   */
  public static final String CFG_ZOOKEEPER_ENSEMBLE = "zookeeper.quorum";
  public static final String CFG_LOCAL_DATA_DIR = "local.data.dir";
  public static final String CFG_YARN_USER = "yarn.user";
  public static final String CFG_HDFS_USER = "hdfs.user";
  public static final String CFG_HDFS_NAMESPACE = "hdfs.namespace";
  public static final String CFG_HDFS_LIB_DIR = "hdfs.lib.dir";

  public static final String CFG_WEAVE_ZK_NAMESPACE = "weave.zookeeper.namespace";
  public static final String CFG_WEAVE_RESERVED_MEMORY_MB = "weave.java.reserved.memory.mb";
  public static final String CFG_WEAVE_NO_CONTAINER_TIMEOUT = "weave.no.container.timeout";
  public static final String CFG_WEAVE_JVM_GC_OPTS = "weave.jvm.gc.opts";

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

  /** Minimum count of table write ops executed by tx to try to apply batching logic to. */
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

  /**
   * Configuration for Metadata service.
   */
  public static final String CFG_METADATA_SERVER_ADDRESS = "metadata.bind.address";
  public static final String CFG_METADATA_SERVER_PORT = "metadata.bind.port";
  public static final String CFG_METADATA_SERVER_THREADS = "metadata.server.threads";

  public static final String CFG_RUN_HISTORY_KEEP_DAYS = "metadata.program.run.history.keepdays";
  public static final int DEFAULT_RUN_HISTORY_KEEP_DAYS = 30;

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

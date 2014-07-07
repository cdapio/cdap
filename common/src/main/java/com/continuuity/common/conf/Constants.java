package com.continuuity.common.conf;

import java.util.concurrent.TimeUnit;

/**
 * Constants used by different systems are all defined here.
 */
public final class Constants {
  /**
   * Global Service names.
   */
  public static final class Service {
    public static final String APP_FABRIC_HTTP = "appfabric";
    public static final String TRANSACTION = "transaction";
    public static final String METRICS = "metrics";
    public static final String LOGSAVER = "log.saver";
    public static final String GATEWAY = "gateway";
    public static final String STREAMS = "streams";
    public static final String REACTOR_SERVICES = "reactor.services";
    public static final String METRICS_PROCESSOR = "metrics.processor";
    public static final String DATASET_MANAGER = "dataset.service";
    public static final String DATASET_EXECUTOR = "dataset.executor";
    public static final String EXTERNAL_AUTHENTICATION = "external.authentication";
    public static final String EXPLORE_HTTP_USER_SERVICE = "explore.service";
    public static final String SERVICE_INSTANCE_TABLE_NAME = "reactor.services.instances";
  }

  /**
   * Zookeeper Configuration.
   */
  public static final class Zookeeper {
    public static final String QUORUM = "zookeeper.quorum";
    public static final String CFG_SESSION_TIMEOUT_MILLIS = "zookeeper.session.timeout.millis";
    public static final int DEFAULT_SESSION_TIMEOUT_MILLIS = 40000;
  }

  /**
   * HBase configurations.
   */
  public static final class HBase {
    public static final String AUTH_KEY_UPDATE_INTERVAL = "hbase.auth.key.update.interval";
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

    //TODO: THis temp
    public static final String DEFAULT_SERVER_ADDRESS = "localhost";

    /**
     * App Fabric Server.
     */
    public static final String SERVER_ADDRESS = "app.bind.address";
    public static final String OUTPUT_DIR = "app.output.dir";
    public static final String TEMP_DIR = "app.temp.dir";
    public static final String REST_PORT = "app.rest.port";
    public static final String PROGRAM_JVM_OPTS = "app.program.jvm.opts";

    /**
     * Query parameter to indicate start time.
     */
    public static final String QUERY_PARAM_START_TIME = "before";

    /**
     * Query parameter to indicate end time.
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

    public static final String SERVICE_DESCRIPTION = "Service for managing application lifecycle.";
  }

  /**
   * Scheduler options.
   */
  public class Scheduler {
    public static final String CFG_SCHEDULER_MAX_THREAD_POOL_SIZE = "scheduler.max.thread.pool.size";
    public static final int DEFAULT_THREAD_POOL_SIZE = 30;
  }

  /**
   * Transactions.
   */
  public static final class Transaction {
    /**
     * Twill Runnable configuration.
     */
    public static final class Container {
      public static final String ADDRESS = "data.tx.bind.address";
      public static final String NUM_INSTANCES = "data.tx.num.instances";
      public static final String NUM_CORES = "data.tx.num.cores";
      public static final String MEMORY_MB = "data.tx.memory.mb";
      public static final String MAX_INSTANCES = "data.tx.max.instances";
    }

    public static final String SERVICE_DESCRIPTION = "Service that maintains transaction states.";

  }

  /**
   * Datasets.
   */
  public static final class Dataset {
    /**
     * DatasetManager service configuration.
     */
    public static final class Manager {
      /** for the address (hostname) of the dataset server. */
      public static final String ADDRESS = "dataset.service.bind.address";

      public static final String BACKLOG_CONNECTIONS = "dataset.service.connection.backlog";
      public static final String EXEC_THREADS = "dataset.service.exec.threads";
      public static final String BOSS_THREADS = "dataset.service.boss.threads";
      public static final String WORKER_THREADS = "dataset.service.worker.threads";
      public static final String OUTPUT_DIR = "dataset.service.output.dir";

      // Defaults
      public static final int DEFAULT_BACKLOG = 20000;
      public static final int DEFAULT_EXEC_THREADS = 10;
      public static final int DEFAULT_BOSS_THREADS = 1;
      public static final int DEFAULT_WORKER_THREADS = 4;
    }

    /**
     * Twill Runnable configuration.
     */
    public static final class Container {
      public static final String NUM_INSTANCES = "dataset.service.num.instances";
      public static final String NUM_CORES = "dataset.service.num.cores";
      public static final String MEMORY_MB = "dataset.service.memory.mb";
    }

    /**
     * DatasetUserService configuration.
     */
    public static final class Executor {
      /** for the port of the dataset user service server. */
      public static final String PORT = "dataset.executor.bind.port";

      /** for the address (hostname) of the dataset server. */
      public static final String ADDRESS = "dataset.executor.bind.address";

      public static final String BACKLOG_CONNECTIONS = "dataset.executor.connection.backlog";
      public static final String EXEC_THREADS = "dataset.executor.exec.threads";
      public static final String BOSS_THREADS = "dataset.executor.boss.threads";
      public static final String WORKER_THREADS = "dataset.executor.worker.threads";
      public static final String OUTPUT_DIR = "dataset.executor.output.dir";

      /** Twill Runnable configuration **/
      public static final String CONTAINER_VIRTUAL_CORES = "dataset.executor.container.num.cores";
      public static final String CONTAINER_MEMORY_MB = "dataset.executor.container.memory.mb";
      public static final String CONTAINER_INSTANCES = "dataset.executor.container.instances";

      //max-instances of dataset executor service
      public static final String MAX_INSTANCES = "dataset.executor.max.instances";

      public static final String SERVICE_DESCRIPTION = "Service to perform Dataset operations.";
    }
  }

  /**
   * Stream configurations.
   */
  public static final class Stream {
    /* Begin CConfiguration keys */
    public static final String BASE_DIR = "stream.base.dir";
    public static final String TTL = "stream.event.ttl";
    public static final String PARTITION_DURATION = "stream.partition.duration";
    public static final String INDEX_INTERVAL = "stream.index.interval";
    public static final String FILE_PREFIX = "stream.file.prefix";
    public static final String CONSUMER_TABLE_PRESPLITS = "stream.consumer.table.presplits";
    public static final String FILE_CLEANUP_PERIOD = "stream.file.cleanup.period";

    // Stream http service configurations.
    public static final String STREAM_HANDLER = "stream.handler";
    public static final String ADDRESS = "stream.bind.address";
    public static final String WORKER_THREADS = "stream.worker.threads";

    // YARN container configurations.
    public static final String CONTAINER_VIRTUAL_CORES = "stream.container.num.cores";
    public static final String CONTAINER_MEMORY_MB = "stream.container.memory.mb";
    public static final String CONTAINER_INSTANCES = "stream.container.instances";

    // Tell the instance id of the YARN container. Set by the StreamHandlerRunnable only, not in default.xml
    public static final String CONTAINER_INSTANCE_ID = "stream.container.instance.id";
    /* End CConfiguration keys */

    /* Begin constants used by stream */

    /** How often to check for new file when reading from stream in milliseconds. **/
    public static final long NEW_FILE_CHECK_INTERVAL = TimeUnit.SECONDS.toMillis(10);
    public static final int HBASE_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;


    /**
     * Contains HTTP headers used by Stream handler.
     */
    public static final class Headers {
      public static final String CONSUMER_ID = "X-Continuuity-ConsumerId";
    }

    // Time for a stream consumer to timeout in StreamHandler for REST API dequeue.
    public static final long CONSUMER_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

    // The consumer state table namespace for consumers created from stream handler for REST API dequeue.
    public static final String HANDLER_CONSUMER_NS = "http.stream.consumer";

    //max instances of stream handler service
    public static final String MAX_INSTANCES = "stream.container.instances";

    public static final String SERVICE_DESCRIPTION = "Service that handles stream data ingestion.";
    /* End constants used by stream */
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
    public static final String HEADER_DESTINATION_STREAM = "X-Continuuity-Destination";
    public static final String HEADER_FROM_COLLECTOR = "X-Continuuity-FromCollector";
    public static final String CONTINUUITY_API_KEY = "X-Continuuity-ApiKey";
    public static final String CFG_PASSPORT_SERVER_URI = "passport.server.uri";
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
   * Metrics constants.
   */
  public static final class Metrics {
    public static final String DATASET_CONTEXT = "-.dataset";
    public static final String ADDRESS = "metrics.bind.address";
    public static final String CLUSTER_NAME = "metrics.cluster.name";
    public static final String CONFIG_AUTHENTICATION_REQUIRED = "metrics.authenticate";
    public static final String BACKLOG_CONNECTIONS = "metrics.connection.backlog";
    public static final String EXEC_THREADS = "metrics.exec.threads";
    public static final String BOSS_THREADS = "metrics.boss.threads";
    public static final String WORKER_THREADS = "metrics.worker.threads";
    public static final String NUM_INSTANCES = "metrics.num.instances";
    public static final String NUM_CORES = "metrics.num.cores";
    public static final String MEMORY_MB = "metrics.memory.mb";
    public static final String MAX_INSTANCES = "metrics.max.instances";
    public static final String SERVICE_DESCRIPTION = "Service to handle metrics requests.";
  }

  /**
   * Configurations for metrics processor.
   */
  public static final class MetricsProcessor {
    public static final String NUM_INSTANCES = "metrics.processor.num.instances";
    public static final String NUM_CORES = "metrics.processor.num.cores";
    public static final String MEMORY_MB = "metrics.processor.memory.mb";
    public static final String MAX_INSTANCES = "metrics.processor.max.instances";

    public static final String METRICS_PROCESSOR_STATUS_HANDLER = "metrics.processor.status.handler";
    public static final String ADDRESS = "metrics.processor.status.bind.address";

    public static final String SERVICE_DESCRIPTION = "Service to process application and system metrics.";
  }

  /**
   * Configurations for log saver.
   */
  public static final class LogSaver {
    public static final String NUM_INSTANCES = "log.saver.num.instances";
    public static final String MEMORY_MB = "log.saver.run.memory.megs";
    public static final String MAX_INSTANCES = "log.saver.max.instances";

    public static final String LOG_SAVER_STATUS_HANDLER = "log.saver.status.handler";
    public static final String ADDRESS = "log.saver.status.bind.address";

    public static final String SERVICE_DESCRIPTION = "Service to collect and store logs.";
  }

  /**
   * Monitor constants.
   */
  public static final class Monitor {
    public static final String STATUS_OK = "OK";
    public static final String STATUS_NOTOK = "NOTOK";
    public static final String DISCOVERY_TIMEOUT_SECONDS = "monitor.handler.service.discovery.timeout.seconds";
  }

  /**
   * Logging constants.
   */
  public static final class Logging {
    public static final String SYSTEM_NAME = "reactor";
    public static final String COMPONENT_NAME = "services";
  }

  /**
   * Security configuration.
   */
  public static final class Security {
    /** Algorithm used to generate the digest for access tokens. */
    public static final String TOKEN_DIGEST_ALGO = "security.token.digest.algorithm";
    /** Key length for secret key used by token digest algorithm. */
    public static final String TOKEN_DIGEST_KEY_LENGTH = "security.token.digest.keylength";
    /** Time duration in milliseconds after which an active secret key should be retired. */
    public static final String TOKEN_DIGEST_KEY_EXPIRATION = "security.token.digest.key.expiration.ms";
    /** Parent znode used for secret key distribution in ZooKeeper. */
    public static final String DIST_KEY_PARENT_ZNODE = "security.token.distributed.parent.znode";
    /** Address the Authentication Server should bind to*/
    public static final String AUTH_SERVER_ADDRESS = "security.auth.server.address";
    /** Configuration for External Authentication Server. */
    public static final String AUTH_SERVER_PORT = "security.auth.server.port";
    /** Maximum number of handler threads for the Authentication Server embedded Jetty instance. */
    public static final String MAX_THREADS = "security.server.maxthreads";
    /** Access token expiration time in milliseconds. */
    public static final String TOKEN_EXPIRATION = "security.server.token.expiration.ms";
    /** Long lasting Access token expiration time in milliseconds. */
    public static final String EXTENDED_TOKEN_EXPIRATION = "security.server.extended.token.expiration.ms";
    public static final String CFG_FILE_BASED_KEYFILE_PATH = "security.data.keyfile.path";
    /** Configuration for enabling the security. */
    public static final String CFG_SECURITY_ENABLED = "security.enabled";
    /** Configuration for security realm. */
    public static final String CFG_REALM = "security.realm";
    /** Authentication Handler class name */
    public static final String AUTH_HANDLER_CLASS = "security.authentication.handlerClassName";
    /** Prefix for all configurable properties of an Authentication handler. */
    public static final String AUTH_HANDLER_CONFIG_BASE = "security.authentication.handler.";
    /** Authentication Login Module class name */
    public static final String LOGIN_MODULE_CLASS_NAME = "security.authentication.loginmodule.className";
    /** Configuration for enabling SSL */
    public static final String SSL_ENABLED = "security.server.ssl.enabled";
    /** SSL secured port for ExternalAuthentication */
    public static final String AUTH_SERVER_SSL_PORT = "security.server.ssl.port";
    /** SSL keystore path */
    public static final String SSL_KEYSTORE_PATH = "security.server.ssl.keystore.path";
    /** SSL keystore password */
    public static final String SSL_KEYSTORE_PASSWORD = "security.server.ssl.keystore.password";
    /** Realm file for Basic Authentication */
    public static final String BASIC_REALM_FILE = "security.authentication.basic.realmfile";
  }

  /**
   * Explore module configuration.
   */
  public static final class Explore {
    public static final String CCONF_KEY = "reactor.cconfiguration";
    public static final String HCONF_KEY = "reactor.hconfiguration";
    public static final String TX_QUERY_KEY = "reactor.hive.query.tx.id";

    public static final String DATASET_NAME = "reactor.dataset.name";
    public static final String DATASET_STORAGE_HANDLER_CLASS = "com.continuuity.hive.datasets.DatasetStorageHandler";
    public static final String EXPLORE_CLASSPATH = "explore.classpath";
    public static final String EXPLORE_CONF_FILES = "explore.conf.files";

    public static final String SERVER_ADDRESS = "explore.service.bind.address";

    public static final String BACKLOG_CONNECTIONS = "explore.service.connection.backlog";
    public static final String EXEC_THREADS = "explore.service.exec.threads";
    public static final String WORKER_THREADS = "explore.service.worker.threads";

    /** Twill Runnable configuration **/
    public static final String CONTAINER_VIRTUAL_CORES = "explore.executor.container.num.cores";
    public static final String CONTAINER_MEMORY_MB = "explore.executor.container.memory.mb";
    public static final String CONTAINER_INSTANCES = "explore.executor.container.instances";

    public static final String CFG_LOCAL_DATA_DIR = "hive.local.data.dir";
    public static final String CFG_EXPLORE_ENABLED = "reactor.explore.enabled";

    //max-instances of explore HTTP service
    public static final String MAX_INSTANCES = "explore.executor.max.instances";

    public static final String ACTIVE_OPERATION_TIMEOUT_SECS = "explore.active.operation.timeout.secs";
    public static final String INACTIVE_OPERATION_TIMEOUT_SECS = "explore.inactive.operation.timeout.secs";
    public static final String CLEANUP_JOB_SCHEDULE_SECS = "explore.cleanup.job.schedule.secs";

    public static final String SERVICE_DESCRIPTION = "Service to run Ad-hoc queries.";

    /**
     * Explore JDBC constants.
     */
    public static final class Jdbc {
      public static final String URL_PREFIX = "jdbc:reactor://";
    }
  }

  public static final String CFG_LOCAL_DATA_DIR = "local.data.dir";
  public static final String CFG_YARN_USER = "yarn.user";
  public static final String CFG_HDFS_USER = "hdfs.user";
  public static final String CFG_HDFS_NAMESPACE = "hdfs.namespace";
  public static final String CFG_HDFS_LIB_DIR = "hdfs.lib.dir";

  public static final String CFG_TWILL_ZK_NAMESPACE = "weave.zookeeper.namespace";
  public static final String CFG_TWILL_RESERVED_MEMORY_MB = "weave.java.reserved.memory.mb";
  public static final String CFG_TWILL_NO_CONTAINER_TIMEOUT = "weave.no.container.timeout";

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
  public static final String CFG_RUN_HISTORY_KEEP_DAYS = "metadata.program.run.history.keepdays";
  public static final int DEFAULT_RUN_HISTORY_KEEP_DAYS = 30;

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

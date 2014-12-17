/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.conf;

import java.util.concurrent.TimeUnit;

/**
 * Constants used by different systems are all defined here.
 */
public final class Constants {

  /**
   * Global Service names.
   */
  public static final class Service {
    public static final String ACL = "acl";
    public static final String APP_FABRIC_HTTP = "appfabric";
    public static final String TRANSACTION = "transaction";
    public static final String METRICS = "metrics";
    public static final String LOGSAVER = "log.saver";
    public static final String GATEWAY = "gateway";
    public static final String STREAMS = "streams";
    public static final String MASTER_SERVICES = "master.services";
    public static final String METRICS_PROCESSOR = "metrics.processor";
    public static final String DATASET_MANAGER = "dataset.service";
    public static final String DATASET_EXECUTOR = "dataset.executor";
    public static final String EXTERNAL_AUTHENTICATION = "external.authentication";
    public static final String EXPLORE_HTTP_USER_SERVICE = "explore.service";
    public static final String SERVICE_INSTANCE_TABLE_NAME = "cdap.services.instances";
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
     * App Fabric Server.
     */
    public static final String SERVER_ADDRESS = "app.bind.address";
    public static final String OUTPUT_DIR = "app.output.dir";
    public static final String TEMP_DIR = "app.temp.dir";
    public static final String REST_PORT = "app.rest.port";
    public static final String PROGRAM_JVM_OPTS = "app.program.jvm.opts";
    public static final String BACKLOG_CONNECTIONS = "app.connection.backlog";
    public static final String EXEC_THREADS = "app.exec.threads";
    public static final String BOSS_THREADS = "app.boss.threads";
    public static final String WORKER_THREADS = "app.worker.threads";

    /**
     * Defaults.
     */
    public static final int DEFAULT_BACKLOG = 20000;
    public static final int DEFAULT_EXEC_THREADS = 20;
    public static final int DEFAULT_BOSS_THREADS = 1;
    public static final int DEFAULT_WORKER_THREADS = 10;

    /**
     * Query parameter to indicate start time.
     */
    public static final String QUERY_PARAM_START_TIME = "start";

    /**
     * Query parameter to indicate status of a program {active, completed, failed}
     */
    public static final String QUERY_PARAM_STATUS = "status";

    /**
     * Query parameter to indicate end time.
     */
    public static final String QUERY_PARAM_END_TIME = "end";

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
      // TODO: This is duplicated from TxConstants. Needs to be removed.
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

    public static final String TABLE_PREFIX = "dataset.table.prefix";

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
    public static final String ASYNC_WORKER_THREADS = "stream.async.worker.threads";
    public static final String ASYNC_QUEUE_SIZE = "stream.async.queue.size";

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
      public static final String CONSUMER_ID = "X-ConsumerId";
    }

    //max instances of stream handler service
    public static final String MAX_INSTANCES = "stream.container.instances";

    public static final String SERVICE_DESCRIPTION = "Service that handles stream data ingestion.";
    /* End constants used by stream */
  }

  /**
   * Gateway Configurations.
   */
  public static final class Gateway {
    public static final String API_VERSION_2_TOKEN = "v2";
    public static final String API_VERSION_2 = "/" + API_VERSION_2_TOKEN;
    /**
     * v3 API.
     */
    public static final String API_VERSION_3_TOKEN = "v3";
    public static final String API_VERSION_3 = "/" + API_VERSION_3_TOKEN;
    public static final String STREAM_HANDLER_NAME = "stream.rest";
    public static final String METRICS_CONTEXT = "gateway." + Gateway.STREAM_HANDLER_NAME;
    public static final String API_KEY = "X-ApiKey";
  }

  /**
   * Router Configuration.
   */
  public static final class Router {
    public static final String ADDRESS = "router.bind.address";
    public static final String ROUTER_PORT = "router.bind.port";
    public static final String WEBAPP_PORT = "router.webapp.bind.port";
    public static final String WEBAPP_ENABLED = "router.webapp.enabled";
    public static final String ROUTER_SSL_PORT = "router.ssl.bind.port";
    public static final String WEBAPP_SSL_PORT = "router.ssl.webapp.bind.port";
    public static final String BACKLOG_CONNECTIONS = "router.connection.backlog";
    public static final String SERVER_BOSS_THREADS = "router.server.boss.threads";
    public static final String SERVER_WORKER_THREADS = "router.server.worker.threads";
    public static final String CLIENT_BOSS_THREADS = "router.client.boss.threads";
    public static final String CLIENT_WORKER_THREADS = "router.client.worker.threads";

    /**
     * Defaults.
     */
    public static final String DEFAULT_ROUTER_PORT = "10000";
    public static final String DEFAULT_WEBAPP_PORT = "20000";
    public static final String DEFAULT_ROUTER_SSL_PORT = "10443";
    public static final String DEFAULT_WEBAPP_SSL_PORT = "20443";
    public static final boolean DEFAULT_WEBAPP_ENABLED = false;


    public static final int DEFAULT_BACKLOG = 20000;
    public static final int DEFAULT_SERVER_BOSS_THREADS = 1;
    public static final int DEFAULT_SERVER_WORKER_THREADS = 10;
    public static final int DEFAULT_CLIENT_BOSS_THREADS = 1;
    public static final int DEFAULT_CLIENT_WORKER_THREADS = 10;

    public static final String GATEWAY_DISCOVERY_NAME = Service.GATEWAY;
    public static final String WEBAPP_DISCOVERY_NAME = "webapp/$HOST";
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

    /**
     * Metric's dataset related constants.
     */
    public static final class Dataset {
      /** Defines reporting interval for HBase stats, in seconds */
      public static final String HBASE_STATS_REPORT_INTERVAL = "metrics.dataset.hbase.stats.report.interval";
      /** Defines reporting interval for LevelDB stats, in seconds */
      public static final String LEVELDB_STATS_REPORT_INTERVAL = "metrics.dataset.leveldb.stats.report.interval";
    }
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
    public static final String SYSTEM_NAME = "cdap";
    public static final String COMPONENT_NAME = "services";
  }

  /**
   * Security configuration.
   */
  public static final class Security {
    /** Enables security. */
    public static final String ENABLED = "security.enabled";
    /** Enables authorization. */
    public static final String AUTHORIZATION_ENABLED = "security.authorization.enabled";
    /** Enables Kerberos authentication. */
    public static final String KERBEROS_ENABLED = "kerberos.auth.enabled";
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
    public static final String AUTH_SERVER_PORT = "security.auth.server.bind.port";
    /** Maximum number of handler threads for the Authentication Server embedded Jetty instance. */
    public static final String MAX_THREADS = "security.server.maxthreads";
    /** Access token expiration time in milliseconds. */
    public static final String TOKEN_EXPIRATION = "security.server.token.expiration.ms";
    /** Long lasting Access token expiration time in milliseconds. */
    public static final String EXTENDED_TOKEN_EXPIRATION = "security.server.extended.token.expiration.ms";
    public static final String CFG_FILE_BASED_KEYFILE_PATH = "security.data.keyfile.path";
    /** Configuration for security realm. */
    public static final String CFG_REALM = "security.realm";
    /** Authentication Handler class name */
    public static final String AUTH_HANDLER_CLASS = "security.authentication.handlerClassName";
    /** Prefix for all configurable properties of an Authentication handler. */
    public static final String AUTH_HANDLER_CONFIG_BASE = "security.authentication.handler.";
    /** Authentication Login Module class name */
    public static final String LOGIN_MODULE_CLASS_NAME = "security.authentication.loginmodule.className";
    /** Realm file for Basic Authentication */
    public static final String BASIC_REALM_FILE = "security.authentication.basic.realmfile";
    /** Enables SSL */
    public static final String SSL_ENABLED = "ssl.enabled";

    /**
     * Headers for security.
     */
    public static final class Headers {
      /** Internal user ID header passed from Router to downstream services */
      public static final String USER_ID = "CDAP-UserId";
    }

    /**
     * Security configuration for Router.
     */
    public static final class Router {
      /** SSL keystore location */
      public static final String SSL_KEYSTORE_PATH = "router.ssl.keystore.path";
      /** SSL keystore type */
      public static final String SSL_KEYSTORE_TYPE = "router.ssl.keystore.type";
      /** SSL keystore key password */
      public static final String SSL_KEYPASSWORD = "router.ssl.keystore.keypassword";
      /** SSL keystore password */
      public static final String SSL_KEYSTORE_PASSWORD = "router.ssl.keystore.password";

      /** Default SSL keystore type */
      public static final String DEFAULT_SSL_KEYSTORE_TYPE = "JKS";
    }

    /**
     * Security configuration for ExternalAuthenticationServer.
     */
    public static final class AuthenticationServer {
      /** SSL port */
      public static final String SSL_PORT = "security.auth.server.ssl.bind.port";
      /** SSL keystore location */
      public static final String SSL_KEYSTORE_PATH = "security.auth.server.ssl.keystore.path";
      /** SSL keystore type */
      public static final String SSL_KEYSTORE_TYPE = "security.auth.server.ssl.keystore.type";
      /** SSL keystore key password */
      public static final String SSL_KEYPASSWORD = "security.auth.server.ssl.keystore.keypassword";
      /** SSL keystore password */
      public static final String SSL_KEYSTORE_PASSWORD = "security.auth.server.ssl.keystore.password";

      /** Default SSL keystore type */
      public static final String DEFAULT_SSL_KEYSTORE_TYPE = "JKS";
    }

    /** Path to the Kerberos keytab file used by CDAP */
    public static final String CFG_CDAP_MASTER_KRB_KEYTAB_PATH = "cdap.master.kerberos.keytab";
    /** Kerberos principal used by CDAP */
    public static final String CFG_CDAP_MASTER_KRB_PRINCIPAL = "cdap.master.kerberos.principal";
  }

  /**
   * Explore module configuration.
   */
  public static final class Explore {
    public static final String CCONF_KEY = "explore.cconfiguration";
    public static final String HCONF_KEY = "explore.hconfiguration";
    public static final String TX_QUERY_KEY = "explore.hive.query.tx.id";
    public static final String TX_QUERY_CLOSED = "explore.hive.query.tx.commited";

    public static final String START_ON_DEMAND = "explore.start.on.demand";

    public static final String DATASET_NAME = "explore.dataset.name";
    public static final String DATASET_STORAGE_HANDLER_CLASS = "co.cask.cdap.hive.datasets.DatasetStorageHandler";
    public static final String STREAM_NAME = "explore.stream.name";
    public static final String STREAM_STORAGE_HANDLER_CLASS = "co.cask.cdap.hive.stream.StreamStorageHandler";
    public static final String EXPLORE_CLASSPATH = "explore.classpath";
    public static final String EXPLORE_CONF_FILES = "explore.conf.files";
    public static final String PREVIEWS_DIR_NAME = "explore.previews.dir";

    public static final String SERVER_ADDRESS = "explore.service.bind.address";

    public static final String BACKLOG_CONNECTIONS = "explore.service.connection.backlog";
    public static final String EXEC_THREADS = "explore.service.exec.threads";
    public static final String WORKER_THREADS = "explore.service.worker.threads";

    /** Twill Runnable configuration **/
    public static final String CONTAINER_VIRTUAL_CORES = "explore.executor.container.num.cores";
    public static final String CONTAINER_MEMORY_MB = "explore.executor.container.memory.mb";
    public static final String CONTAINER_INSTANCES = "explore.executor.container.instances";

    public static final String LOCAL_DATA_DIR = "explore.local.data.dir";
    public static final String EXPLORE_ENABLED = "explore.enabled";
    public static final String WRITES_ENABLED = "explore.writes.enabled";

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
      public static final String URL_PREFIX = "jdbc:cdap://";
    }
  }

  public static final String CFG_LOCAL_DATA_DIR = "local.data.dir";
  public static final String CFG_YARN_USER = "yarn.user";
  public static final String CFG_HDFS_USER = "hdfs.user";
  public static final String CFG_HDFS_NAMESPACE = "hdfs.namespace";
  public static final String CFG_HDFS_LIB_DIR = "hdfs.lib.dir";

  public static final String CFG_TWILL_ZK_NAMESPACE = "twill.zookeeper.namespace";
  public static final String CFG_TWILL_RESERVED_MEMORY_MB = "twill.java.reserved.memory.mb";
  public static final String CFG_TWILL_NO_CONTAINER_TIMEOUT = "twill.no.container.timeout";

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
   * Default namespace to be used by v2 APIs
   */
  public static final String DEFAULT_NAMESPACE = "default";

  /**
   * Constants related to external systems.
   */
  public static final class External {
    /**
     * Constants used by Java security.
     */
    public static final class JavaSecurity {
      public static final String ENV_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
    }

    /**
     * Constants used by Zookeeper.
     */
    public static final class Zookeeper {
      public static final String ENV_AUTH_PROVIDER_1 = "zookeeper.authProvider.1";
      public static final String ENV_ALLOW_SASL_FAILED_CLIENTS = "zookeeper.allowSaslFailedClients";
    }
  }

  /**
   * Constants for the dashboard/frontend.
   */
  public static final class Dashboard {
    /**
     * Port for the dashboard to bind to in non-SSL mode.
     */
    public static final String BIND_PORT = "dashboard.bind.port";

    /**
     * Port for the dashboard to bind to in SSL mode.
     */
    public static final String SSL_BIND_PORT = "dashboard.ssl.bind.port";

    /**
     * True to allow self-signed SSL certificates for endpoints accessed by the dashboard.
     */
    public static final String SSL_ALLOW_SELFSIGNEDCERT = "dashboard.selfsignedcertificate.enabled";
  }

  /**
   * Constants for endpoints
   */
  public static final class EndPoints {
    /**
    * Status endpoint
    */
    public static final String STATUS = "/status";
  }
}

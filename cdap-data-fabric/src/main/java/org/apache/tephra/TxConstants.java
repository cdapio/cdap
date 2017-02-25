/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra;

import co.cask.cdap.common.conf.Constants;
import org.apache.tephra.snapshot.DefaultSnapshotCodec;
import org.apache.tephra.snapshot.SnapshotCodecV2;
import org.apache.tephra.snapshot.SnapshotCodecV3;
import org.apache.tephra.snapshot.SnapshotCodecV4;

import java.util.concurrent.TimeUnit;

/**
 * Transaction system constants
 */
public class TxConstants {
  /**
   * Defines what level of conflict detection should be used for transactions.  {@code ROW} means that only the
   * table name and row key for each change will be used to determine if the transaction change sets conflict.
   * {@code COLUMN} means that the table name, row key, column family, and column qualifier will all be used to
   * identify write conflicts.  {@code NONE} means that no conflict detection will be performed, but transaction
   * clients will still track the current transaction's change set to rollback any persisted changes in the event of
   * a failure.  This should only be used where writes to the same coordinate should never conflict, such as
   * append-only data.  The default value used by {@code TransactionAwareHTable} implementations is {@code COLUMN}.
   *
   * <p>
   * <strong>Note: for a given table, all clients must use the same conflict detection setting!</strong>
   * Otherwise conflicts will not be flagged correctly.
   * </p>
   */
  public enum ConflictDetection {
    ROW,
    COLUMN,
    NONE
  }

  /**
   * Property set for {@code org.apache.hadoop.hbase.HColumnDescriptor} to configure time-to-live on data within
   * the column family.  The value given is in milliseconds.  Once a cell's data has surpassed the given value in age,
   * the cell's data will no longer be visible and may be garbage collected.
   */
  public static final String PROPERTY_TTL = "dataset.table.ttl";

  /**
   * This is how many tx we allow per millisecond, if you care about the system for 100 years:
   * Long.MAX_VALUE / (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(365 * 100)) =
   * (as of Feb 20, 2014) 2,028,653. It is safe and convenient to use 1,000,000 as multiplier:
   * <ul>
   *   <li>
   *     we hardly can do more than 1 billion txs per second
   *   </li>
   *   <li>
   *     long value will not overflow for 200 years
   *   </li>
   *   <li>
   *     makes reading & debugging easier if multiplier is 10^n
   *   </li>
   * </ul>
   */
  public static final long MAX_TX_PER_MS = 1000000;

  /**
   * Since HBase {@code Delete} operations cannot be undone at the same timestamp, "deleted" data is instead
   * overwritten with an empty {@code byte[]} to flag it as removed.  Cells with empty values will be filtered out
   * of the results for read operations.  If cells with empty values should be included in results (meaning data
   * cannot be transactionally deleted), then set this configuration property to true.
   */
  public static final String ALLOW_EMPTY_VALUES_KEY = "data.tx.allow.empty.values";
  public static final boolean ALLOW_EMPTY_VALUES_DEFAULT = false;

  /**
   * Key used to set the serialized transaction as an attribute on Get and Scan operations.
   */
  public static final String TX_OPERATION_ATTRIBUTE_KEY = "tephra.tx";
  /**
   * @deprecated This constant is replaced by {@link #TX_OPERATION_ATTRIBUTE_KEY}
   */
  public static final String OLD_TX_OPERATION_ATTRIBUTE_KEY = "cask.tx";
  /**
   * Key used to flag a delete operation as part of a transaction rollback.  This is used so that the
   * {@code TransactionProcessor} coprocessor loaded on a table can differentiate between deletes issued
   * as part of a normal client operation versus those performed when rolling back a transaction.
   */
  public static final String TX_ROLLBACK_ATTRIBUTE_KEY = "tephra.tx.rollback";
  /**
   * @deprecated This constant is replaced by {@link #TX_ROLLBACK_ATTRIBUTE_KEY}
   */
  public static final String OLD_TX_ROLLBACK_ATTRIBUTE_KEY = "cask.tx.rollback";

  /**
   * Column qualifier used for a special delete marker tombstone, which identifies an entire column family as deleted.
   */
  public static final byte[] FAMILY_DELETE_QUALIFIER = new byte[0];

  // Constants for monitoring status
  public static final String STATUS_OK = "OK";
  public static final String STATUS_NOTOK = "NOTOK";

  /**
   * Indicates whether data written before Tephra was enabled on a table should be readable. Reading non-transactional
   * data can lead to slight performance penalty. Hence it is disabled by default.
   * @see <a href="https://issues.cask.co/browse/TEPHRA-89">TEPHRA-89</a>
   */
  public static final String READ_NON_TX_DATA = "data.tx.read.pre.existing";
  public static final boolean DEFAULT_READ_NON_TX_DATA = false;

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
    /** The user id to access HDFS if not running in secure HDFS. */
    public static final String CFG_TX_HDFS_USER = "data.tx.hdfs.user";
    /** Default value for how often to check in-progress transactions for expiration, in seconds. */
    public static final int DEFAULT_TX_CLEANUP_INTERVAL = 10;
    /**
     * The timeout for a transaction, in seconds. If the transaction is not finished in that time,
     * it is marked invalid.
     */
    public static final String CFG_TX_TIMEOUT = "data.tx.timeout";
    /** Default value for transaction timeout, in seconds. */
    public static final int DEFAULT_TX_TIMEOUT = 30;
    /**
     * The timeout for a long running transaction, in seconds. If the transaction is not finished in that time,
     * it is marked invalid.
     */
    public static final String CFG_TX_LONG_TIMEOUT = "data.tx.long.timeout";
    /** Default value for long running transaction timeout, in seconds. */
    public static final int DEFAULT_TX_LONG_TIMEOUT = (int) TimeUnit.DAYS.toSeconds(1);
    /**
     * The limit for the allowed transaction timeout, in seconds. Attempts to start a transaction with a longer
     * timeout will fail.
     */
    public static final String CFG_TX_MAX_TIMEOUT = "data.tx.max.timeout";
    /**
     * The default value for the transaction timeout limit, in seconds: unlimited.
     */
    public static final int DEFAULT_TX_MAX_TIMEOUT = Integer.MAX_VALUE;
    /**
     * The maximum time in seconds that a transaction can be used for data writes.
     */
    public static final String CFG_TX_MAX_LIFETIME = Constants.Tephra.CFG_TX_MAX_LIFETIME;
    /**
     * The default value for the maximum transaction lifetime.
     */
    public static final int DEFAULT_TX_MAX_LIFETIME = Constants.Tephra.DEFAULT_TX_MAX_LIFETIME;
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

    /** for the zookeeper quorum string for leader election for tx server. */
    public static final String CFG_DATA_TX_ZOOKEEPER_QUORUM
      = "data.tx.zookeeper.quorum";

    /** for the name used to announce service availability to discovery service */
    public static final String CFG_DATA_TX_DISCOVERY_SERVICE_NAME
      = "data.tx.discovery.service.name";

    /** for the port of the tx server. */
    public static final String CFG_DATA_TX_BIND_PORT
      = "data.tx.bind.port";

    /** for the address (hostname) of the tx server. */
    public static final String CFG_DATA_TX_BIND_ADDRESS
      = "data.tx.bind.address";

    /** the number of IO threads in the tx service. */
    public static final String CFG_DATA_TX_SERVER_IO_THREADS
      = "data.tx.server.io.threads";

    /** the number of handler threads in the tx service. */
    public static final String CFG_DATA_TX_SERVER_THREADS
      = "data.tx.server.threads";

    public static final String CFG_DATA_TX_THRIFT_MAX_READ_BUFFER
      = "data.tx.thrift.max.read.buffer";

    public static final String DEFAULT_DATA_TX_DISCOVERY_SERVICE_NAME
      = "transaction";

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

    /** default thrift max read buffer size */
    public static final int DEFAULT_DATA_TX_THRIFT_MAX_READ_BUFFER
      = 16 * 1024 * 1024;

    // Configuration key names and defaults used by tx client.

    /** to specify the tx client socket timeout in ms. */
    public static final String CFG_DATA_TX_CLIENT_TIMEOUT
      = "data.tx.client.timeout";

    /** to specify the tx client provider strategy. */
    public static final String CFG_DATA_TX_CLIENT_PROVIDER
      = "data.tx.client.provider";

    /** to specify the number of threads for client provider "pool". */
    public static final String CFG_DATA_TX_CLIENT_COUNT
      = "data.tx.client.count";

    /** timeout (in milliseconds) for obtaining client from client provider "pool". */
    public static final String CFG_DATA_TX_CLIENT_OBTAIN_TIMEOUT_MS
      = "data.tx.client.obtain.timeout";

    /** to specify the retry strategy for a failed thrift call. */
    public static final String CFG_DATA_TX_CLIENT_RETRY_STRATEGY
      = "data.tx.client.retry.strategy";

    /** to specify the number of times to retry a failed thrift call. */
    public static final String CFG_DATA_TX_CLIENT_ATTEMPTS
      = "data.tx.client.retry.attempts";

    /** to specify the initial sleep time for retry strategy backoff. */
    public static final String CFG_DATA_TX_CLIENT_BACKOFF_INITIAL
      = "data.tx.client.retry.backoff.initial";

    /** to specify the backoff factor for retry strategy backoff. */
    public static final String CFG_DATA_TX_CLIENT_BACKOFF_FACTOR
      = "data.tx.client.retry.backoff.factor";

    /** to specify the sleep time limit for retry strategy backoff. */
    public static final String CFG_DATA_TX_CLIENT_BACKOFF_LIMIT
      = "data.tx.client.retry.backoff.limit";

    /** the default tx client socket timeout in milli seconds. */
    public static final int DEFAULT_DATA_TX_CLIENT_TIMEOUT_MS
      = 30 * 1000;

    /** default number of tx clients for client provider "pool". */
    public static final int DEFAULT_DATA_TX_CLIENT_COUNT
      = 50;

    /** default timeout (in milliseconds) for obtaining client from client provider "pool". */
    public static final long DEFAULT_DATA_TX_CLIENT_OBTAIN_TIMEOUT_MS
      = TimeUnit.SECONDS.toMillis(3);

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
    public static final int DEFAULT_DATA_TX_CLIENT_BACKOFF_INITIAL
      = 100;

    /** default backoff factor is 4. */
    public static final int DEFAULT_DATA_TX_CLIENT_BACKOFF_FACTOR
      = 4;

    /** default sleep limit is 30 sec. */
    public static final int DEFAULT_DATA_TX_CLIENT_BACKOFF_LIMIT
      = 30 * 1000;
  }

  /**
   * Configuration properties for metrics reporting
   */
  public static final class Metrics {
    /**
     * Frequency at which metrics should be reported, in seconds.
     */
    public static final String REPORT_PERIOD_KEY = "data.tx.metrics.period";
    /**
     * Default report period for metrics, in seconds.
     */
    public static final int REPORT_PERIOD_DEFAULT = 60;
  }

  /**
   * Configuration properties used by HBase
   */
  public static final class HBase {
    public static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout";
    public static final int DEFAULT_ZK_SESSION_TIMEOUT = 180 * 1000;
  }

  /**
   * Configuration for the transaction snapshot persistence.
   */
  public static final class Persist {
    /**
     * The class names of all known transaction snapshot codecs.
     */
    public static final String CFG_TX_SNAPHOT_CODEC_CLASSES = "data.tx.snapshot.codecs";
    public static final Class[] DEFAULT_TX_SNAPHOT_CODEC_CLASSES = 
      { DefaultSnapshotCodec.class, SnapshotCodecV2.class, SnapshotCodecV3.class, SnapshotCodecV4.class };
  }

  /**
   * Configuration for transaction log edit entries
   */
  public static final class TransactionLog {
    /**
     * Key used to denote the number of entries appended.
     */
    public static final String NUM_ENTRIES_APPENDED = "count";
    public static final String VERSION_KEY = "version";
    public static final byte CURRENT_VERSION = 3;
  }

  /**
   * Configuration for invalid transaction pruning
   */
  public static final class TransactionPruning {
    /**
     * Flag to enable automatic invalid transaction pruning.
     */
    public static final String PRUNE_ENABLE = "data.tx.prune.enable";
    /**
     * The table used to store intermediate state when pruning is enabled.
     */
    public static final String PRUNE_STATE_TABLE = "data.tx.prune.state.table";
    /**
     * Interval in seconds to schedule prune run.
     */
    public static final String PRUNE_INTERVAL = "data.tx.prune.interval";

    /**
     * Interval in seconds to schedule flush of prune table entries to store.
     */
    public static final String PRUNE_FLUSH_INTERVAL = "data.tx.prune.flush.interval";

    /**
     * The time in seconds used to pad transaction max lifetime while pruning.
     */
    public static final String PRUNE_GRACE_PERIOD = "data.tx.grace.period";

    /**
     * Comma separated list of invalid transaction pruning plugins to load
     */
    public static final String PLUGINS = "data.tx.prune.plugins";
    /**
     * Class name for the plugins will be plugin-name + ".class" suffix
     */
    public static final String PLUGIN_CLASS_SUFFIX = ".class";

    public static final boolean DEFAULT_PRUNE_ENABLE = false;
    public static final String DEFAULT_PRUNE_STATE_TABLE = "tephra.state";
    public static final long DEFAULT_PRUNE_INTERVAL = TimeUnit.HOURS.toSeconds(6);
    public static final long DEFAULT_PRUNE_FLUSH_INTERVAL = TimeUnit.MINUTES.toSeconds(1);
    public static final long DEFAULT_PRUNE_GRACE_PERIOD = TimeUnit.HOURS.toSeconds(24);
    public static final String DEFAULT_PLUGIN = "data.tx.prune.plugin.default";
    public static final String DEFAULT_PLUGIN_CLASS =
      "org.apache.tephra.hbase.txprune.HBaseTransactionPruningPlugin";
  }
}

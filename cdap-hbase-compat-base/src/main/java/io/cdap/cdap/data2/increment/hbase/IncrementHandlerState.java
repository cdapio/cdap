/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.data2.increment.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.transaction.coprocessor.DefaultTransactionStateCacheSupplier;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TxConstants;
import org.apache.tephra.coprocessor.CacheSupplier;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.persist.TransactionVisibilityState;

/**
 * Common state and utilities shared by the HBase version-specific {@code IncrementHandler}
 * coprocessor implementations.  This common implementation cannot go into a shared base class, as
 * each coprocessor needs to derive from the HBase version's {@code BaseRegionObserver} class, in
 * order to avoid being broken by API changes.
 */
public class IncrementHandlerState {

  /**
   * Property set for {@link HColumnDescriptor} to indicate if increment is transactional. Default:
   * "true", i.e. transactional.
   */
  public static final String PROPERTY_TRANSACTIONAL = "dataset.table.readless.increment.transactional";
  public static final long MAX_TS_PER_MS = 1000000;
  // prefix bytes used to mark values that are deltas vs. full sums
  public static final byte[] DELTA_MAGIC_PREFIX = new byte[]{'X', 'D'};
  // expected length for values storing deltas (prefix + increment value)
  public static final int DELTA_FULL_LENGTH = DELTA_MAGIC_PREFIX.length + Bytes.SIZEOF_LONG;
  public static final int BATCH_UNLIMITED = -1;

  public static final Log LOG = LogFactory.getLog(IncrementHandlerState.class);
  private final HTableDescriptor hTableDescriptor;
  private final CoprocessorEnvironment env;

  protected final Set<byte[]> txnlFamilies = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
  protected Map<byte[], Long> ttlByFamily = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

  private CacheSupplier<TransactionStateCache> txStateCacheSupplier;
  private TransactionStateCache cache;

  private TimestampOracle timeOracle = new TimestampOracle();

  public IncrementHandlerState(CoprocessorEnvironment env, HTableDescriptor hTableDescriptor) {
    this.env = env;
    this.hTableDescriptor = hTableDescriptor;
  }

  public void stop() {
    if (txStateCacheSupplier != null) {
      txStateCacheSupplier.release();
    }
  }

  @VisibleForTesting
  public void setTimestampOracle(TimestampOracle timeOracle) {
    this.timeOracle = timeOracle;
  }

  private CacheSupplier<TransactionStateCache> getTransactionStateCacheSupplier() {
    String tablePrefix = hTableDescriptor.getValue(Constants.Dataset.TABLE_PREFIX);
    return new DefaultTransactionStateCacheSupplier(tablePrefix, env);
  }

  public void initFamily(byte[] familyName, Map<byte[], byte[]> familyValues) {
    String familyAsString = Bytes.toString(familyName);
    byte[] transactionalConfig = familyValues.get(
        Bytes.toBytes(IncrementHandlerState.PROPERTY_TRANSACTIONAL));
    boolean txnl =
        transactionalConfig == null || !"false".equals(Bytes.toString(transactionalConfig));
    LOG.info("Family " + familyAsString + " is transactional: " + txnl);
    if (txnl) {
      txnlFamilies.add(familyName);
    }

    // check for TTL configuration
    byte[] columnTTL = familyValues.get(Bytes.toBytes(TxConstants.PROPERTY_TTL));
    long ttl = 0;
    if (columnTTL != null) {
      try {
        String stringTTL = Bytes.toString(columnTTL);
        ttl = Long.parseLong(stringTTL);
        LOG.info("Family " + familyAsString + " has TTL of " + ttl);
      } catch (NumberFormatException nfe) {
        LOG.warn("Invalid TTL value configured for column family " + familyAsString
            + ", value = " + Bytes.toStringBinary(columnTTL));
      }
    }
    ttlByFamily.put(familyName, ttl);

    // get the transaction state cache as soon as we have a transactional family
    if (!txnlFamilies.isEmpty() && cache == null) {
      txStateCacheSupplier = getTransactionStateCacheSupplier();
      cache = txStateCacheSupplier.get();
    }
  }

  public boolean containsTransactionalFamily(Set<byte[]> familyNames) {
    // we assume that if any of the column families written to are transactional, the entire write is transactional
    for (byte[] key : familyNames) {
      if (txnlFamilies.contains(key)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a unique timestamp for the current operation.
   *
   * @return a new timestamp guaranteed to be unique within the scope of usage
   */
  public synchronized long getUniqueTimestamp() {
    return timeOracle.getUniqueTimestamp();
  }

  /**
   * Returns the upper bound beyond which we can compact any increment deltas into a new sum.
   *
   * @param columnFamily the column family name
   * @return the newest timestamp beyond which can compact delta increments
   */
  public long getCompactionBound(byte[] columnFamily) {
    if (txnlFamilies.contains(columnFamily)) {
      TransactionVisibilityState snapshot = cache.getLatestState();
      // if tx snapshot is not available, used "0" as upper bound to avoid trashing in-progress tx
      return snapshot != null ? snapshot.getVisibilityUpperBound() : 0;
    } else {
      return Long.MAX_VALUE;
    }
  }

  /**
   * Returns the time-to-live (in milliseconds) for the given column family, transformed into the
   * same precision used in assigning unique timestamps.
   *
   * @param familyName the column family name
   * @return the time-to-live value
   */
  public long getFamilyTTL(byte[] familyName) {
    Long configuredTTL = ttlByFamily.get(familyName);
    return configuredTTL == null ? -1 : configuredTTL * TxConstants.MAX_TX_PER_MS;
  }

  /**
   * Returns the oldest timestamp that will be visible for a given column family, after the column
   * family's configured time-to-live is applied.
   *
   * @param familyName the name of the column family
   * @return the oldest timestamp value that will be visible
   */
  public long getOldestVisibleTimestamp(byte[] familyName) {
    long familyTTL = getFamilyTTL(familyName);
    return familyTTL > 0 ? timeOracle.currentTime() - familyTTL : 0;
  }
}

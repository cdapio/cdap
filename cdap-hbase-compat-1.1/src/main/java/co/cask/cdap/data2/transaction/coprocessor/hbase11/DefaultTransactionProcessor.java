/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.coprocessor.hbase11;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTable;
import co.cask.cdap.data2.increment.hbase11.IncrementTxFilter;
import co.cask.cdap.data2.transaction.coprocessor.CConfigurationCache;
import co.cask.cdap.data2.transaction.coprocessor.CConfigurationCacheSupplier;
import co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCacheSupplier;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.Transaction;
import org.apache.tephra.TxConstants;
import org.apache.tephra.coprocessor.CacheSupplier;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.hbase.coprocessor.CellSkipFilter;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;
import org.apache.tephra.util.TxUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link org.apache.tephra.hbase.coprocessor.TransactionProcessor}
 * coprocessor that uses {@link co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCache}
 * to automatically refresh transaction state.
 */
public class DefaultTransactionProcessor extends TransactionProcessor {
  private static final Log LOG = LogFactory.getLog(DefaultTransactionProcessor.class);

  private CConfigurationCacheSupplier cConfCacheSupplier;
  private CConfigurationCache cConfCache;
  private String sysConfigTablePrefix;

  private Cache<Long, Transaction> txCache;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      HTableDescriptor tableDesc = env.getRegion().getTableDesc();
      String hbaseNamespacePrefix = tableDesc.getValue(Constants.Dataset.TABLE_PREFIX);

      this.sysConfigTablePrefix = HTableNameConverter.getSysConfigTablePrefix(hbaseNamespacePrefix);
      this.cConfCacheSupplier = new CConfigurationCacheSupplier(env.getConfiguration(), sysConfigTablePrefix,
                                                                TxConstants.Manager.CFG_TX_MAX_LIFETIME,
                                                                TxConstants.Manager.DEFAULT_TX_MAX_LIFETIME);
      this.cConfCache = cConfCacheSupplier.get();
      this.txCache = CacheBuilder.newBuilder()
        .maximumSize(10)
        .expireAfterAccess(30, TimeUnit.SECONDS)
        .build();
      LOG.info(getClass().getSimpleName() + " loaded with txCache for table '" + tableDesc.getNameAsString() + "'");
    }
    // Need to create the cConf cache before calling start on the parent, since it is required for
    // initializing some properties in the parent class.
    super.start(e);
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    super.stop(e);
    cConfCacheSupplier.release();
  }

  @Override
  public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    reloadPruneState(e.getEnvironment());
    super.postFlush(e);
  }

  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
                                               List<? extends KeyValueScanner> scanners, ScanType scanType,
                                               long earliestPutTs, InternalScanner s,
                                               CompactionRequest request) throws IOException {
    reloadPruneState(c.getEnvironment());
    return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s, request);
  }

  @Override
  protected void ensureValidTxLifetime(RegionCoprocessorEnvironment env, OperationWithAttributes op,
                                       @Nullable Transaction tx) throws IOException {
    if (tx == null) {
      return;
    }

    long currMaxLifetimeinMillis = getCurrMaxLifetime(env, op, cConfCache.getTxMaxLifetimeMillis());
    // Since we don't always set the txMaxLifetimeMillis variable (in case when we get the info from the client)
    // we need to compute whether the transaction is within the validLifetime boundary here instead of
    // relying on the parent
    boolean validLifetime =
      (TxUtils.getTimestamp(tx.getTransactionId()) + currMaxLifetimeinMillis) > System.currentTimeMillis();
    if (!validLifetime) {
      throw new DoNotRetryIOException(String.format("Transaction %s has exceeded max lifetime %s ms",
                                                    tx.getTransactionId(), currMaxLifetimeinMillis));
    }
  }

  private long getCurrMaxLifetime(RegionCoprocessorEnvironment env, OperationWithAttributes op,
                                  @Nullable Long maxLifetimeFromConf) {
    // If conf has the value, update the txMaxLifetimeMillis and return it
    if (maxLifetimeFromConf != null) {
      txMaxLifetimeMillis = maxLifetimeFromConf;
      return maxLifetimeFromConf;
    }

    // This case shouldn't happen but in case current value from conf is null but it was set earlier, return that
    if (txMaxLifetimeMillis != null) {
      return txMaxLifetimeMillis;
    }

    // If value from conf is null, derive it from the attribute set by the client
    byte[] maxLifetimeBytes = op.getAttribute(HBaseTable.TX_MAX_LIFETIME_MILLIS_KEY);
    if (maxLifetimeBytes != null) {
      return Bytes.toLong(maxLifetimeBytes);
    }

    // If maxLifetimeMillis could not be found in the client request as well, just use the default value
    long defaultMaxLifeTimeInSecs = TxConstants.Manager.DEFAULT_TX_MAX_LIFETIME;
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("txMaxLifetimeMillis is not available in client's operation attributes. " +
                                "Defaulting to the default value of %d seconds for region %s.",
                              defaultMaxLifeTimeInSecs, env.getRegionInfo().getRegionNameAsString()));
    }
    return TimeUnit.SECONDS.toMillis(defaultMaxLifeTimeInSecs);
  }

  private void reloadPruneState(RegionCoprocessorEnvironment env) {
    if (pruneEnable == null) {
      // If prune enable has never been initialized, try to do so now
      initializePruneState(env);
    } else {
      Configuration conf = getConfiguration(env);
      if (conf != null) {
        boolean newPruneEnable = conf.getBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE,
                                                 TxConstants.TransactionPruning.DEFAULT_PRUNE_ENABLE);
        if (newPruneEnable != pruneEnable) {
          // pruning enable has been changed, resetting prune state
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Transaction Invalid List pruning feature is set to %s now for region %s.",
                                    newPruneEnable, env.getRegionInfo().getRegionNameAsString()));
          }
          resetPruneState();
          initializePruneState(env);
        }
      }
    }
  }

  @Override
  @Nullable
  protected Configuration getConfiguration(CoprocessorEnvironment env) {
    return cConfCache.getConf();
  }

  @Override
  protected CacheSupplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    return new DefaultTransactionStateCacheSupplier(sysConfigTablePrefix, env.getConfiguration());
  }

  @Override
  protected Filter getTransactionFilter(Transaction tx, ScanType scanType, Filter filter) {
    IncrementTxFilter incrementTxFilter = new IncrementTxFilter(tx, ttlByFamily, allowEmptyValues, scanType, filter);
    return new CellSkipFilter(incrementTxFilter);
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
    throws IOException {
    Transaction tx = getTxFromOperation(get);
    if (tx != null) {
      projectFamilyDeletes(get);
      get.setMaxVersions();
      get.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx, readNonTxnData),
                       TxUtils.getMaxVisibleTimestamp(tx));
      Filter newFilter = getTransactionFilter(tx, ScanType.USER_SCAN, get.getFilter());
      get.setFilter(newFilter);
    }
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    Transaction tx = getTxFromOperation(scan);
    if (tx != null) {
      projectFamilyDeletes(scan);
      scan.setMaxVersions();
      scan.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx, readNonTxnData),
                        TxUtils.getMaxVisibleTimestamp(tx));
      Filter newFilter = getTransactionFilter(tx, ScanType.USER_SCAN, scan.getFilter());
      scan.setFilter(newFilter);
    }
    return s;
  }

  /**
   * Ensures that family delete markers are present in the columns requested for any scan operation.
   * @param scan The original scan request
   * @return The modified scan request with the family delete qualifiers represented
   */
  private Scan projectFamilyDeletes(Scan scan) {
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
      NavigableSet<byte[]> columns = entry.getValue();
      // wildcard scans will automatically include the delete marker, so only need to add it when we have
      // explicit columns listed
      if (columns != null && !columns.isEmpty()) {
        scan.addColumn(entry.getKey(), TxConstants.FAMILY_DELETE_QUALIFIER);
      }
    }
    return scan;
  }

  /**
   * Ensures that family delete markers are present in the columns requested for any get operation.
   * @param get The original get request
   * @return The modified get request with the family delete qualifiers represented
   */
  private Get projectFamilyDeletes(Get get) {
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : get.getFamilyMap().entrySet()) {
      NavigableSet<byte[]> columns = entry.getValue();
      // wildcard scans will automatically include the delete marker, so only need to add it when we have
      // explicit columns listed
      if (columns != null && !columns.isEmpty()) {
        get.addColumn(entry.getKey(), TxConstants.FAMILY_DELETE_QUALIFIER);
      }
    }
    return get;
  }

  private Transaction getTxFromOperation(final OperationWithAttributes op) throws IOException {
    byte[] wpBytes = op.getAttribute(HBaseTable.WRITE_POINTER);
    // LOG.info("geTxFromOperation: wpBytes is " + Bytes.toStringBinary(wpBytes));
    if (wpBytes == null || wpBytes.length != Bytes.SIZEOF_LONG) {
      LOG.info("Getting tx from operation.");
      return getFromOperation(op);
    }
    final long writePointer = Bytes.toLong(wpBytes);
    try {
      return txCache.get(writePointer, new Callable<Transaction>() {
        @Override
        public Transaction call() throws Exception {
          // LOG.info("Write pointer " + writePointer + " not in cache, getting tx from operation.");
          return getFromOperation(op);
        }
      });
    } catch (ExecutionException e) {
      Throwable t = e.getCause();
      Throwables.propagateIfInstanceOf(t, IOException.class);
      throw Throwables.propagate(t);
    }
  }
}

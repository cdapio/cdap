/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.coprocessor.hbase12cdh570;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTable;
import co.cask.cdap.data2.increment.hbase12cdh570.IncrementTxFilter;
import co.cask.cdap.data2.transaction.coprocessor.CConfigurationCache;
import co.cask.cdap.data2.transaction.coprocessor.CConfigurationCacheSupplier;
import co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCacheSupplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
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
  private String tablePrefix;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      HTableDescriptor tableDesc = env.getRegion().getTableDesc();
      this.tablePrefix = tableDesc.getValue(Constants.Dataset.TABLE_PREFIX);
      this.cConfCacheSupplier = new CConfigurationCacheSupplier(env, tablePrefix,
                                                                TxConstants.Manager.CFG_TX_MAX_LIFETIME,
                                                                TxConstants.Manager.DEFAULT_TX_MAX_LIFETIME);
      this.cConfCache = cConfCacheSupplier.get();
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
    return new DefaultTransactionStateCacheSupplier(tablePrefix, env);
  }

  @Override
  protected Filter getTransactionFilter(Transaction tx, ScanType scanType, Filter filter) {
    IncrementTxFilter incrementTxFilter = new IncrementTxFilter(tx, ttlByFamily, allowEmptyValues, scanType, filter);
    return new CellSkipFilter(incrementTxFilter);
  }
}

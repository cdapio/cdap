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

package co.cask.cdap.data2.transaction.coprocessor.hbase10cdh;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTable;
import co.cask.cdap.data2.increment.hbase10cdh.IncrementTxFilter;
import co.cask.cdap.data2.transaction.coprocessor.CConfigurationCache;
import co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCacheSupplier;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.Transaction;
import org.apache.tephra.TxConstants;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.hbase.coprocessor.CellSkipFilter;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;
import org.apache.tephra.util.TxUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link org.apache.tephra.hbase.coprocessor.TransactionProcessor}
 * coprocessor that uses {@link co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCache}
 * to automatically refresh transaction state.
 */
public class DefaultTransactionProcessor extends TransactionProcessor {
  private static final Log LOG = LogFactory.getLog(DefaultTransactionProcessor.class);

  private CConfigurationCache cConfCache;
  private String sysConfigTablePrefix;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if (e instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
      HTableDescriptor tableDesc = env.getRegion().getTableDesc();
      String hbaseNamespacePrefix = tableDesc.getValue(Constants.Dataset.TABLE_PREFIX);

      this.sysConfigTablePrefix = HTableNameConverter.getSysConfigTablePrefix(hbaseNamespacePrefix);
      this.cConfCache = createCConfCache(env);
    }
    // Need to create the cConf cache before calling start on the parent, since it is required for
    // initializing some properties in the parent class.
    super.start(e);
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    super.stop(e);
    if (cConfCache != null) {
      cConfCache.stop();
    }
  }

  @Override
  protected void ensureValidTxLifetime(RegionCoprocessorEnvironment env, OperationWithAttributes op,
                                       @Nullable Transaction tx) throws IOException {
    if (tx == null) {
      return;
    }

    long maxLifetimeMillis;
    if (txMaxLifetimeMillis == null) {
      Configuration conf = getConfiguration(env);
      if (conf != null) {
        this.txMaxLifetimeMillis = TimeUnit.SECONDS.toMillis(conf.getInt(TxConstants.Manager.CFG_TX_MAX_LIFETIME,
                                                                         TxConstants.Manager.DEFAULT_TX_MAX_LIFETIME));
        maxLifetimeMillis = this.txMaxLifetimeMillis;
      } else {
        // Get maxLifetimeMillis from transaction attributes
        byte[] maxLifetimeBytes = op.getAttribute(HBaseTable.TX_MAX_LIFETIME_MILLIS_KEY);
        if (maxLifetimeBytes != null) {
          maxLifetimeMillis = Bytes.toLong(maxLifetimeBytes);
        } else {
          LOG.warn("txMaxLifetimeMillis is not available in client's operation attributes. " +
                     "Defaulting to default tx_max_lifetime");
          maxLifetimeMillis = TimeUnit.SECONDS.toMillis(TxConstants.Manager.DEFAULT_TX_MAX_LIFETIME);
        }
      }
    } else {
      maxLifetimeMillis = txMaxLifetimeMillis;
    }

    boolean validLifetime =
      TxUtils.getTimestamp(tx.getTransactionId()) + maxLifetimeMillis > System.currentTimeMillis();
    if (!validLifetime) {
      throw new DoNotRetryIOException(String.format("Transaction %s has exceeded max lifetime %s ms",
                                                    tx.getTransactionId(), maxLifetimeMillis));
    }
  }

  private CConfigurationCache getCConfCache(RegionCoprocessorEnvironment env) {
    if (cConfCache == null || (!cConfCache.isAlive())) {
      cConfCache = createCConfCache(env);
    }
    return cConfCache;
  }

  private CConfigurationCache createCConfCache(RegionCoprocessorEnvironment env) {
    return new CConfigurationCache(env.getConfiguration(), sysConfigTablePrefix);
  }

  @Override
  @Nullable
  protected Configuration getConfiguration(CoprocessorEnvironment env) {
    CConfiguration cConf = getCConfCache((RegionCoprocessorEnvironment) env).getCConf();
    if (cConf == null) {
      return null;
    }

    Configuration hConf = new Configuration();
    for (Map.Entry<String, String> entry : cConf) {
      hConf.set(entry.getKey(), entry.getValue());
    }
    return hConf;
  }

  @Override
  protected Supplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    return new DefaultTransactionStateCacheSupplier(sysConfigTablePrefix, env.getConfiguration());
  }

  @Override
  protected Filter getTransactionFilter(Transaction tx, ScanType scanType, Filter filter) {
    IncrementTxFilter incrementTxFilter = new IncrementTxFilter(tx, ttlByFamily, allowEmptyValues, scanType, filter);
    return new CellSkipFilter(incrementTxFilter);
  }
}

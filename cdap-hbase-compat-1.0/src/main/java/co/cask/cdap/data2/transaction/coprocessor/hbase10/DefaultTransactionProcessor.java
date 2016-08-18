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

package co.cask.cdap.data2.transaction.coprocessor.hbase10;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.increment.hbase10.IncrementTxFilter;
import co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCacheSupplier;
import co.cask.cdap.data2.util.hbase.HTable10NameConverter;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.tephra.Transaction;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.hbase.coprocessor.CellSkipFilter;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;

/**
 * Implementation of the {@link org.apache.tephra.hbase.coprocessor.TransactionProcessor}
 * coprocessor that uses {@link co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCache}
 * to automatically refresh transaction state.
 */
public class DefaultTransactionProcessor extends TransactionProcessor {
  @Override
  protected Supplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    HTableDescriptor htd = env.getRegion().getTableDesc();
    String tablePrefix = htd.getValue(Constants.Dataset.TABLE_PREFIX);
    String sysConfigTablePrefix = new HTable10NameConverter().getSysConfigTablePrefix(tablePrefix);
    return new DefaultTransactionStateCacheSupplier(sysConfigTablePrefix, env.getConfiguration());
  }

  @Override
  protected Filter getTransactionFilter(Transaction tx, ScanType scanType, Filter filter) {
    IncrementTxFilter incrementTxFilter = new IncrementTxFilter(tx, ttlByFamily, allowEmptyValues, scanType, filter);
    return new CellSkipFilter(incrementTxFilter);
  }
}

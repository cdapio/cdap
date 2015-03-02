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

package co.cask.cdap.data2.transaction.coprocessor.hbase94;

import co.cask.cdap.data2.increment.hbase94.IncrementFilter;
import co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCacheSupplier;
import co.cask.cdap.data2.util.hbase.HBase94TableUtil;
import co.cask.tephra.Transaction;
import co.cask.tephra.coprocessor.TransactionStateCache;
import co.cask.tephra.hbase94.coprocessor.TransactionProcessor;
import co.cask.tephra.hbase94.coprocessor.TransactionVisibilityFilter;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.ScanType;

/**
 * Implementation of the {@link co.cask.tephra.hbase94.coprocessor.TransactionProcessor}
 * coprocessor that uses {@link co.cask.cdap.data2.transaction.coprocessor.DefaultTransactionStateCache}
 * to automatically refresh transaction state.
 */
public class DefaultTransactionProcessor extends TransactionProcessor {

  @Override
  protected Supplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    String sysConfigTablePrefix
      = new HBase94TableUtil().getSysConfigTablePrefix(env.getRegion().getTableDesc().getNameAsString());
    return new DefaultTransactionStateCacheSupplier(sysConfigTablePrefix, env.getConfiguration());
  }

  @Override
  protected Filter getTransactionFilter(Transaction tx, ScanType scanType) {
    return new TransactionVisibilityFilter(tx, ttlByFamily, allowEmptyValues, scanType, new IncrementFilter());
  }
}

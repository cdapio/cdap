package com.continuuity.data2.transaction.coprocessor.hbase94;

import com.continuuity.data2.transaction.coprocessor.ReactorTransactionStateCacheSupplier;
import com.continuuity.data2.transaction.coprocessor.TransactionStateCache;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * Implementation of the {@link com.continuuity.data2.transaction.coprocessor.hbase94.TransactionDataJanitor}
 * coprocessor that uses {@link com.continuuity.data2.transaction.coprocessor.ReactorTransactionStateCache}
 * to automatically refresh transaction state.
 */
public class ReactorTransactionDataJanitor extends TransactionDataJanitor {

  @Override
  protected Supplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    String tableName = env.getRegion().getTableDesc().getNameAsString();
    String[] parts = tableName.split("\\.", 2);
    String tableNamespace = "";
    if (parts.length > 0) {
      tableNamespace = parts[0];
    }
    return new ReactorTransactionStateCacheSupplier(tableNamespace, env.getConfiguration());
  }
}

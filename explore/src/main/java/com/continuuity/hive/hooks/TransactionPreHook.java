package com.continuuity.hive.hooks;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.hive.context.CConfSerDe;
import com.continuuity.hive.context.ContextManager;
import com.continuuity.hive.context.HConfSerDe;
import com.continuuity.hive.context.TxnSerDe;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hook to wrap a hive query in a transaction, which is passed along to all the datasets
 * involved in the query.
 */
public class TransactionPreHook implements ExecuteWithHookContext {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionPreHook.class);

  public void run(HookContext hookContext) throws Exception {
    LOG.debug("Entering pre hive hook");
    if (hookContext.getOperationName().equals(HiveOperation.QUERY.name())) {
      HiveConf hiveConf = hookContext.getConf();
      TransactionSystemClient txClient = ContextManager.getTxClient(hiveConf);
      Transaction tx = txClient.startLong();

      TxnSerDe.serialize(tx, hiveConf);
      CConfSerDe.serialize(CConfiguration.create(), hiveConf);
      HConfSerDe.serialize(HBaseConfiguration.create(), hiveConf);
    }
  }
}

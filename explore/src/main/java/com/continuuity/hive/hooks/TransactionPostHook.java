package com.continuuity.hive.hooks;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.hive.context.ContextManager;
import com.continuuity.hive.context.TxnSerDe;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hook to retrieve the transaction a query has been wrapped in, and to terminate it - either by
 * committing it or by aborting it.
 */
public class TransactionPostHook implements ExecuteWithHookContext {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionPostHook.class);

  @Override
  public void run(HookContext hookContext) throws Exception {
    // We cannot rely on hookContext.getOperationName(), it remains the same
    // through the life of a beeline command
    if (TransactionPreHook.SELECT_QUERY.matcher(hookContext.getQueryPlan().getQueryString()).matches()) {
      LOG.debug("Entering post hive hook for hive query");
      HiveConf hiveConf = hookContext.getConf();
      Transaction tx = TxnSerDe.deserialize(hiveConf);
      LOG.debug("Transaction retrieved in post hook: {}", tx);

      TransactionSystemClient txClient = ContextManager.getTxClient(hiveConf);
      // Transaction doesn't involve any changes
      if (txClient.canCommit(tx, ImmutableList.<byte[]>of())) {
        if (!txClient.commit(tx)) {
          txClient.abort(tx);
          LOG.info("Could not pass second commit checking for tx used for Hive query: {}", tx);
        }
      } else {
        // Very unlikely with empty changes
        txClient.abort(tx);
        LOG.info("Could not pass first commit checking for tx used for Hive query: {}", tx);
      }
    }
  }
}

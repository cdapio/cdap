package com.continuuity.hive.hooks;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.hive.HiveServer;
import com.continuuity.hive.datasets.DatasetInputFormat;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hook to retrieve the transaction a query has been wrapped in, and to terminate it - either by
 * committing it or by aborting it.
 */
public class TransactionPostHook implements ExecuteWithHookContext {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionPostHook.class);
  private static final Gson GSON = new Gson();

  @Override
  public void run(HookContext hookContext) throws Exception {
    if (hookContext.getOperationName().equals(HiveOperation.QUERY.name())) {
      HiveConf hiveConf = hookContext.getConf();
      String txJson = hiveConf.get(DatasetInputFormat.TX_QUERY);
      Preconditions.checkNotNull(txJson, "Transaction ID not set for Hive query.");
      Transaction tx = GSON.fromJson(txJson, Transaction.class);
      LOG.debug("Transaction retrieved in post hook: {}", tx);

      TransactionSystemClient txClient = HiveServer.getTransactionSystemClient();
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

package com.continuuity.hive.hooks;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.hive.datasets.DatasetInputFormat;
import com.continuuity.hive.server.RuntimeHiveServer;

import com.google.gson.Gson;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * Hook to wrap a hive query in a transaction, which is passed along to all the datasets
 * involved in the query.
 */
public class TransactionPreHook implements ExecuteWithHookContext {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionPreHook.class);
  private static final Gson GSON = new Gson();
  static final Pattern SELECT_QUERY = Pattern.compile("^(\\s)*select\\s.*$",
                                                      Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

  public void run(HookContext hookContext) throws Exception {
    // We cannot rely on hookContext.getOperationName(), it remains the same
    // through the life of a beeline command
    if (SELECT_QUERY.matcher(hookContext.getQueryPlan().getQueryString()).matches()) {
      LOG.debug("Entering pre hive hook for hive query.");
      TransactionSystemClient txClient = RuntimeHiveServer.getTransactionSystemClient();
      Transaction tx = txClient.startLong();
      HiveConf hiveConf = hookContext.getConf();
      hiveConf.set(DatasetInputFormat.TX_QUERY, GSON.toJson(tx));
      LOG.debug("Transaction set in pre hook to: {}", tx);
    }
  }
}

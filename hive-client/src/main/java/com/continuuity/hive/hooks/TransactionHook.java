package com.continuuity.hive.hooks;

import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.hive.inmemory.LocalHiveServer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hooks executed in Hive driver before and after executing any command.
 */
// todo TransactionSystemClient should be retrieved differently/won't work in distributed mode.
public class TransactionHook implements ExecuteWithHookContext {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionHook.class);

  private static final Gson GSON = new Gson();
  private static final Pattern QUERY_PATTERN = Pattern.compile("^select\\s.*$",
                                                               Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

  @Override
  public void run(HookContext hookContext) throws Exception {
    if (hookContext.getHookType().equals(HookContext.HookType.PRE_EXEC_HOOK)) {
      runPreHook(hookContext);
    } else if (hookContext.getHookType().equals(HookContext.HookType.POST_EXEC_HOOK)) {
      runPostHook(hookContext);
    }
  }

  /**
   * Hook executed before hive command execution.
   * @param hookContext
   * @throws Exception
   */
  private void runPreHook(HookContext hookContext) throws Exception {
    // Hack to make sure the command is a query
    if (QUERY_PATTERN.matcher(hookContext.getQueryPlan().getQueryString()).matches()) {
      TransactionSystemClient txClient = LocalHiveServer.getTransactionSystemClient();
      Transaction tx = txClient.startLong();
      HiveConf hiveConf = hookContext.getConf();
      hiveConf.set(Constants.Hive.TX_QUERY, GSON.toJson(tx));
      LOG.debug("Transaction set in pre hook to: {}", tx);
    }
  }

  /**
   * Hook executed after hive command execution.
   * @param hookContext
   * @throws Exception
   */
  private void runPostHook(HookContext hookContext) throws Exception {
    // Hack to make sure the command is a query
    if (QUERY_PATTERN.matcher(hookContext.getQueryPlan().getQueryString()).matches()) {
      HiveConf hiveConf = hookContext.getConf();
      String txJson = hiveConf.get(Constants.Hive.TX_QUERY);
      Preconditions.checkNotNull(txJson, "Transaction ID not set for Hive query.");
      Transaction tx = new Gson().fromJson(txJson, Transaction.class);
      LOG.debug("Transaction retrieved in post hook: {}", tx);

      TransactionSystemClient txClient = LocalHiveServer.getTransactionSystemClient();
      // Transaction doesn't involve any changes
      if (txClient.canCommit(tx, ImmutableList.<byte[]>of())) {
        if (!txClient.commit(tx)) {
          txClient.abort(tx);
        }
      } else {
        // Very unlikely with empty changes
        txClient.abort(tx);
      }
    }
  }
}

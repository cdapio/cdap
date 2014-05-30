package com.continuuity.hive.hooks;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.hive.context.CConfCodec;
import com.continuuity.hive.context.ConfigurationUtil;
import com.continuuity.hive.context.ContextManager;
import com.continuuity.hive.context.HConfCodec;
import com.continuuity.hive.context.TxnCodec;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
  static final Pattern SELECT_QUERY = Pattern.compile("^(\\s)*select\\s.*$",
                                                      Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

  public void run(HookContext hookContext) throws Exception {
    // We cannot rely on hookContext.getOperationName(), it remains the same
    // through the life of a beeline command
    if (SELECT_QUERY.matcher(hookContext.getQueryPlan().getQueryString()).matches()) {
      LOG.debug("Entering pre hive hook for hive query.");
      HiveConf hiveConf = hookContext.getConf();
      TransactionSystemClient txClient = ContextManager.getTxClient(hiveConf);
      Transaction tx = txClient.startLong();

      ConfigurationUtil.set(hiveConf, Constants.Explore.TX_QUERY_CODEC_KEY, TxnCodec.INSTANCE, tx);
      ConfigurationUtil.set(hiveConf, Constants.Explore.CCONF_CODEC_KEY, CConfCodec.INSTANCE,
                            CConfiguration.create());
      ConfigurationUtil.set(hiveConf, Constants.Explore.HCONF_CODEC_KEY, HConfCodec.INSTANCE,
                            HBaseConfiguration.create());
    }
  }
}

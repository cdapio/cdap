package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An tx client provider that uses thread local to maintain at most one open connection per thread.
 */
public class ThreadLocalClientProvider extends AbstractClientProvider {

  private static final Logger Log =
      LoggerFactory.getLogger(ThreadLocalClientProvider.class);

  ThreadLocal<TransactionServiceThriftClient> clients =
      new ThreadLocal<TransactionServiceThriftClient>();

  public ThreadLocalClientProvider(CConfiguration conf) {
    super(conf);
  }

  @Override
  public TransactionServiceThriftClient getClient() throws TException {
    TransactionServiceThriftClient client = this.clients.get();
    if (client == null) {
      try {
        client = this.newClient();
        clients.set(client);
      } catch (TException e) {
        Log.error("Unable to create new opex client for thread: "
            + e.getMessage());
        throw e;
      }
  }
    return client;
  }

  @Override
  public void returnClient(TransactionServiceThriftClient client) {
    // nothing to do
  }

  @Override
  public void discardClient(TransactionServiceThriftClient client) {
    client.close();
    clients.remove();
  }

  @Override
  public String toString() {
    return "Thread-local";
  }
}

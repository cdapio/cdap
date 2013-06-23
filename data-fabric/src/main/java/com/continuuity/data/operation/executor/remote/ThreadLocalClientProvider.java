package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An opex client provider that uses thread local to maintain at most one open connection per thread.
 */
public class ThreadLocalClientProvider extends AbstractClientProvider {

  private static final Logger Log =
      LoggerFactory.getLogger(ThreadLocalClientProvider.class);

  ThreadLocal<OperationExecutorClient> clients =
      new ThreadLocal<OperationExecutorClient>();

  public ThreadLocalClientProvider(CConfiguration conf) {
    super(conf);
  }

  @Override
  public OperationExecutorClient getClient() throws TException {
    OperationExecutorClient client = this.clients.get();
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
  public void returnClient(OperationExecutorClient client) {
    // nothing to do
  }

  @Override
  public void discardClient(OperationExecutorClient client) {
    client.close();
    clients.remove();
  }

  @Override
  public String toString() {
    return "Thread-local";
  }
}

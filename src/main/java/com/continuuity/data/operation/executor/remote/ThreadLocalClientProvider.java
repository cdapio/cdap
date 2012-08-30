package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadLocalClientProvider extends AbstractClientProvider {

  private static final Logger Log =
      LoggerFactory.getLogger(ThreadLocalClientProvider.class);

  ThreadLocal<OperationExecutorClient> clients =
      new ThreadLocal<OperationExecutorClient>();

  public ThreadLocalClientProvider(CConfiguration conf) {
    super(conf);
  }

  @Override
  public OperationExecutorClient getClient() {
    OperationExecutorClient client = this.clients.get();
    if (client == null) {
      try {
        client = this.newClient();
        clients.set(client);
      } catch (Exception e) {
        Log.error("Unable to create new opex client for thread: "
            + e.getMessage());
        // TODO we need
        return null;
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

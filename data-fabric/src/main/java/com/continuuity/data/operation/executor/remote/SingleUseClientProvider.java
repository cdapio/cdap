package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An opex client provider that creates a new connection every time.
 */
public class SingleUseClientProvider extends AbstractClientProvider {

  private static final Logger Log =
      LoggerFactory.getLogger(SingleUseClientProvider.class);

  public SingleUseClientProvider(CConfiguration conf, int timeout) {
    super(conf);
    this.timeout = timeout;
  }

  final int timeout;

  @Override
  public OperationExecutorClient getClient() throws TException {
    try {
      return this.newClient(timeout);
    } catch (TException e) {
      Log.error("Unable to create new opex client: " + e.getMessage());
      throw e;
    }
  }

  @Override
  public void returnClient(OperationExecutorClient client) {
    discardClient(client);
  }

  @Override
  public void discardClient(OperationExecutorClient client) {
    client.close();
  }

  @Override
  public String toString() {
    return "Single-use(timeout = " + timeout + ")";
  }
}

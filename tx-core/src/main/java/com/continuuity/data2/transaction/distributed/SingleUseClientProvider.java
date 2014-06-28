package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.conf.CConfiguration;
import org.apache.thrift.TException;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An tx client provider that creates a new connection every time.
 */
public class SingleUseClientProvider extends AbstractClientProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(SingleUseClientProvider.class);

  public SingleUseClientProvider(CConfiguration conf, DiscoveryServiceClient discoveryServiceClient, int timeout) {
    super(conf, discoveryServiceClient);
    this.timeout = timeout;
  }

  final int timeout;

  @Override
  public TransactionServiceThriftClient getClient() throws TException {
    try {
      return this.newClient(timeout);
    } catch (TException e) {
      LOG.error("Unable to create new tx client: " + e.getMessage());
      throw e;
    }
  }

  @Override
  public void returnClient(TransactionServiceThriftClient client) {
    discardClient(client);
  }

  @Override
  public void discardClient(TransactionServiceThriftClient client) {
    client.close();
  }

  @Override
  public String toString() {
    return "Single-use(timeout = " + timeout + ")";
  }
}

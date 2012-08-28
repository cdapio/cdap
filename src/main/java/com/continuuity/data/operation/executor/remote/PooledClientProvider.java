package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PooledClientProvider extends AbstractClientProvider {

  private static final Logger Log =
      LoggerFactory.getLogger(PooledClientProvider.class);

  // we will use this as a pool of opex clients
  class OpexClientPool extends ElasticPool<OperationExecutorClient> {

    OpexClientPool(int sizeLimit) {
      super(sizeLimit);
    }

    @Override
    protected OperationExecutorClient create() {
      try {
        return newClient();
      } catch (IOException e) {
        String message =
            "Unable to create new opex client for thread: " + e.getMessage();
        Log.error(message);
        throw new RuntimeException(message, e);
      }
    }

    @Override
    protected void destroy(OperationExecutorClient client) {
      client.close();
    }
  }

  // we will use this as a pool of opex clients
  OpexClientPool clients;

  // the limit for the number of active clients
  int maxClients;

  public PooledClientProvider(CConfiguration conf) {
    super(conf);
  }

  @Override
  public void initialize() throws IOException {
    // initialize the super class (needed for service discovery)
    super.initialize();

    // create a (empty) pool of opex clients
    maxClients = configuration.getInt(Constants.CFG_DATA_OPEX_CLIENT_COUNT,
        Constants.DEFAULT_DATA_OPEX_CLIENT_COUNT);
    if (maxClients < 1) {
      Log.warn("Configuration of " + Constants.CFG_DATA_OPEX_CLIENT_COUNT +
          " is invalid: value is " + maxClients + " but must be at least 1. " +
          "Using 1 as a fallback. ");
      maxClients = 1;
    }
    this.clients = new OpexClientPool(maxClients);
  }

  @Override
  public OperationExecutorClient getClient() {
    return clients.obtain();
  }

  @Override
  public void returnClient(OperationExecutorClient client) {
    clients.release(client);
  }

  @Override
  public void discardClient(OperationExecutorClient client) {
    clients.discard(client);
  }

}

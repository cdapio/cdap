package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class PooledClientProvider extends AbstractClientProvider {

  private static final Logger Log =
      LoggerFactory.getLogger(PooledClientProvider.class);

  // we will use this as a pool of opex clients
  BlockingQueue<OperationExecutorClient> clients;

  public PooledClientProvider(CConfiguration conf) {
    super(conf);
  }

  @Override
  public void initialize() throws IOException {
    // initialize the super class (needed for service discovery)
    super.initialize();

    // create a (empty) pool of opex clients
    int numClients = configuration.getInt(Constants.CFG_DATA_OPEX_CLIENT_COUNT,
        Constants.DEFAULT_DATA_OPEX_CLIENT_COUNT);
    this.clients = new ArrayBlockingQueue<OperationExecutorClient>(numClients);

    // create n clients and add them to the pool
    for (int i = 0; i < numClients; i++) {
      this.clients.add(this.newClient());
    }
    Log.info("Successfully created " + numClients + " operation executor " +
        "clients.");
  }

  @Override
  public OperationExecutorClient getClient() {
    try {
      return this.clients.take();
    } catch (InterruptedException e) {
      Log.error("take() was interrupted. Don't know what to do. Bailing out.");
      throw new RuntimeException("take() interrupted. better bail out.");
    }
  }

  @Override
  public void returnClient(OperationExecutorClient client) {
    this.clients.add(client);
  }
}

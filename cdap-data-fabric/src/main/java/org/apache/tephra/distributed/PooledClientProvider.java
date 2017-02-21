/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.distributed;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TxConstants;
import org.apache.thrift.TException;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is an tx client provider that uses a bounded size pool of connections.
 */
public class PooledClientProvider extends AbstractClientProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(PooledClientProvider.class);

  // we will use this as a pool of tx clients
  class TxClientPool extends ElasticPool<TransactionServiceThriftClient, TException> {
    TxClientPool(int sizeLimit) {
      super(sizeLimit);
    }

    @Override
    protected TransactionServiceThriftClient create() throws TException {
      return newClient();
    }

    @Override
    protected boolean recycle(TransactionServiceThriftClient client) {
      if (!client.isValid()) {
        client.close();
        return false;
      }
      return true;
    }
  }

  // we will use this as a pool of tx clients
  private volatile TxClientPool clients;

  // the limit for the number of active clients
  private int maxClients;
  // timeout, for obtaining a client
  private long obtainClientTimeoutMs;

  public PooledClientProvider(Configuration conf, DiscoveryServiceClient discoveryServiceClient) {
    super(conf, discoveryServiceClient);
  }

  private void initializePool() throws TException {
    // initialize the super class (needed for service discovery)
    super.initialize();

    // create a (empty) pool of tx clients
    maxClients = configuration.getInt(TxConstants.Service.CFG_DATA_TX_CLIENT_COUNT,
                                      TxConstants.Service.DEFAULT_DATA_TX_CLIENT_COUNT);
    if (maxClients < 1) {
      LOG.warn("Configuration of " + TxConstants.Service.CFG_DATA_TX_CLIENT_COUNT +
                 " is invalid: value is " + maxClients + " but must be at least 1. " +
                 "Using 1 as a fallback. ");
      maxClients = 1;
    }

    obtainClientTimeoutMs =
      configuration.getLong(TxConstants.Service.CFG_DATA_TX_CLIENT_OBTAIN_TIMEOUT_MS,
                            TxConstants.Service.DEFAULT_DATA_TX_CLIENT_OBTAIN_TIMEOUT_MS);
    if (obtainClientTimeoutMs < 0) {
      LOG.warn("Configuration of " + TxConstants.Service.CFG_DATA_TX_CLIENT_COUNT +
                 " is invalid: value is " + obtainClientTimeoutMs + " but must be at least 0. " +
                 "Using 0 as a fallback. ");
      obtainClientTimeoutMs = 0;
    }
    this.clients = new TxClientPool(maxClients);
  }

  @Override
  public CloseableThriftClient getCloseableClient() throws TException, TimeoutException, InterruptedException {
    TransactionServiceThriftClient client = getClientPool().obtain(obtainClientTimeoutMs, TimeUnit.MILLISECONDS);
    return new CloseableThriftClient(this, client);
  }

  @Override
  public void returnClient(TransactionServiceThriftClient client) {
    getClientPool().release(client);
  }

  @Override
  public String toString() {
    return "Elastic pool of size " + this.maxClients +
      ", with timeout (in milliseconds): " + this.obtainClientTimeoutMs;
  }

  private TxClientPool getClientPool() {
    if (clients != null) {
      return clients;
    }

    synchronized (this) {
      if (clients == null) {
        try {
          initializePool();
        } catch (TException e) {
          LOG.error("Failed to initialize Tx client provider", e);
          throw Throwables.propagate(e);
        }
      }
    }
    return clients;
  }
}

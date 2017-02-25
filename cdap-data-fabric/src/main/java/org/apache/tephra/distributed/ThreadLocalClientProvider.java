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

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 * An tx client provider that uses thread local to maintain at most one open connection per thread.
 * Note that there can be a connection leak if the threads are recycled.
 */
public class ThreadLocalClientProvider extends AbstractClientProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(ThreadLocalClientProvider.class);

  ThreadLocal<TransactionServiceThriftClient> clients = new ThreadLocal<>();

  public ThreadLocalClientProvider(Configuration conf, DiscoveryServiceClient discoveryServiceClient) {
    super(conf, discoveryServiceClient);
  }

  @Override
  public CloseableThriftClient getCloseableClient() throws TException, TimeoutException, InterruptedException {
    TransactionServiceThriftClient client = this.clients.get();
    if (client == null) {
      try {
        client = this.newClient();
        clients.set(client);
      } catch (TException e) {
        LOG.error("Unable to create new tx client for thread: "
                    + e.getMessage());
        throw e;
      }
    }
    return new CloseableThriftClient(this, client);
  }

  @Override
  public void returnClient(TransactionServiceThriftClient client) {
    if (!client.isValid()) {
      client.close();
      clients.remove();
    }
  }

  @Override
  public String toString() {
    return "Thread-local";
  }
}

/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.data2.transaction.distributed;

import org.apache.hadoop.conf.Configuration;
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

  public SingleUseClientProvider(Configuration conf, DiscoveryServiceClient discoveryServiceClient, int timeout) {
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

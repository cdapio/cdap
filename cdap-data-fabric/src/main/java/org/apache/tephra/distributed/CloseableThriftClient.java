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

/**
 * An {@link AutoCloseable} to automatically return the thrift client to the ThriftClientProvider.
 */
public class CloseableThriftClient implements AutoCloseable {

  private final ThriftClientProvider provider;
  private final TransactionServiceThriftClient thriftClient;

  public CloseableThriftClient(ThriftClientProvider provider, TransactionServiceThriftClient thriftClient) {
    this.provider = provider;
    this.thriftClient = thriftClient;
  }

  public TransactionServiceThriftClient getThriftClient() {
    return thriftClient;
  }

  @Override
  public void close() {
    // in any case, the client must be returned to the pool. The pool is
    // responsible for discarding the client if it is in a bad state.
    provider.returnClient(thriftClient);
  }
}

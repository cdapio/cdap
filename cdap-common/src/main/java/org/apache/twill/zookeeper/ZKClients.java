/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.zookeeper;

import org.apache.twill.internal.zookeeper.FailureRetryZKClient;
import org.apache.twill.internal.zookeeper.NamespaceZKClient;
import org.apache.twill.internal.zookeeper.RewatchOnExpireZKClient;

/**
 * Utility class to create {@link ZKClient} instances.
 */
public final class ZKClients {

  /**
   * Creates a {@link ZKClient} that will perform auto re-watch on all existing watches
   * when reconnection happens after session expiration. All {@link org.apache.zookeeper.Watcher Watchers}
   * set through the returned {@link ZKClient} would not receive any connection events.
   *
   * @param client The {@link ZKClient} for operations delegation.
   * @return A {@link ZKClient} that will do auto re-watch on all methods that accept a
   *        {@link org.apache.zookeeper.Watcher} upon session expiration.
   */
  public static ZKClient reWatchOnExpire(ZKClient client) {
    return new RewatchOnExpireZKClient(client);
  }

  /**
   * Creates a {@link ZKClient} that will retry interim failure (e.g. connection loss, session expiration)
   * based on the given {@link RetryStrategy}.
   *
   * @param client The {@link ZKClient} for operations delegation.
   * @param retryStrategy The {@link RetryStrategy} to be invoke when there is operation failure.
   * @return A {@link ZKClient}.
   */
  public static ZKClient retryOnFailure(ZKClient client, RetryStrategy retryStrategy) {
    return new FailureRetryZKClient(client, retryStrategy);
  }

  /**
   * Creates a {@link ZKClient} that will add prefix namespace for every paths.
   *
   * @param zkClient The {@link ZKClient} for operations delegation.
   * @param namespace The prefix namespace to be prepended to paths.
   * @return A {@link ZKClient} that will add namespace to every path.
   */
  public static ZKClient namespace(ZKClient zkClient, String namespace) {
    return new NamespaceZKClient(zkClient, namespace);
  }

  private ZKClients() {
  }
}

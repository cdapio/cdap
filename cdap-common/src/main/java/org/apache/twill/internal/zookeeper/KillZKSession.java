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
package org.apache.twill.internal.zookeeper;

import com.google.common.base.Preconditions;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for killing ZK client to simulate failures during testing.
 */
public final class KillZKSession {

  /**
   * Utility classes should have a public constructor or a default constructor
   * hence made it private.
   */
  private KillZKSession() {}

  /**
   * Kills a Zookeeper client to simulate failure scenarious during testing.
   * Callee will provide the amount of time to wait before it's considered failure
   * to kill a client.
   *
   * @param client that needs to be killed.
   * @param connectionString of Quorum
   * @param maxMs time in millisecond specifying the max time to kill a client.
   * @throws IOException When there is IO error
   * @throws InterruptedException When call has been interrupted.
   */
  public static void kill(ZooKeeper client, String connectionString,
                          int maxMs) throws IOException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    ZooKeeper zk = new ZooKeeper(connectionString, maxMs, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
          latch.countDown();
        }
      }
    }, client.getSessionId(), client.getSessionPasswd());

    try {
      Preconditions.checkState(latch.await(maxMs, TimeUnit.MILLISECONDS), "Fail to kill ZK connection.");
    } finally {
      zk.close();
    }
  }
}

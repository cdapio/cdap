/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import org.apache.twill.discovery.Discoverable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link TaskManager} warm sticky lease orchestration.
 */
public class TaskManagerTest {

  private List<Discoverable> pods;

  @Before
  public void setUp() {
    pods = new ArrayList<>();
    // Define 3 mock task worker pods
    pods.add(new Discoverable("task.worker", new InetSocketAddress("10.0.0.1", 11015)));
    pods.add(new Discoverable("task.worker", new InetSocketAddress("10.0.0.2", 11015)));
    pods.add(new Discoverable("task.worker", new InetSocketAddress("10.0.0.3", 11015)));
  }

  @Test
  public void testStickyRoutingAndLeasing() {
    TaskManager taskManager = TaskManager.getInstance();

    // 1. First request for ns1 should claim a pod
    Discoverable pod1 = taskManager.resolvePod("ns1", pods);
    Assert.assertNotNull(pod1);

    // 2. Second request for ns1 should land on the same pod (Stickiness)
    Discoverable pod1Repeat = taskManager.resolvePod("ns1", pods);
    Assert.assertEquals(pod1.getSocketAddress(), pod1Repeat.getSocketAddress());

    // 3. First request for ns2 should claim a different, idle pod (Isolation)
    Discoverable pod2 = taskManager.resolvePod("ns2", pods);
    Assert.assertNotNull(pod2);
    Assert.assertNotEquals(pod1.getSocketAddress(), pod2.getSocketAddress());

    // Clean up active tasks
    taskManager.finishTask("ns1", pod1);
    taskManager.finishTask("ns1", pod1Repeat);
    taskManager.finishTask("ns2", pod2);
  }

  @Test
  public void testLogicalResetAfterMaxTasks() {
    TaskManager taskManager = TaskManager.getInstance();

    // 1. Claim a pod for ns3
    Discoverable initialPod = taskManager.resolvePod("ns3", pods);
    Assert.assertNotNull(initialPod);
    taskManager.finishTask("ns3", initialPod);

    // 2. Send 9 more tasks to reach the limit of 10 total tasks
    for (int i = 0; i < 9; i++) {
      Discoverable p = taskManager.resolvePod("ns3", pods);
      Assert.assertEquals(initialPod.getSocketAddress(), p.getSocketAddress());
      taskManager.finishTask("ns3", p);
    }

    // 3. The 11th request for a DIFFERENT namespace (ns4) should now be able to claim this pod
    // because it was logically reset (released) on the 10th task!
    Discoverable resetPod = taskManager.resolvePod("ns4", pods);
    Assert.assertNotNull(resetPod);
    Assert.assertEquals(initialPod.getSocketAddress(), resetPod.getSocketAddress());
    taskManager.finishTask("ns4", resetPod);
  }
}

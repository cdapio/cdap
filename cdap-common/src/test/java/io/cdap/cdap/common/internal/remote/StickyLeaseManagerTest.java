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

import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Unit test verifying Proof of Concept (POC) for Sticky Lease with Lifecycle & Reclamation.
 */
public class StickyLeaseManagerTest {

  @Test
  public void testFirstWriteWinsLeaseAcquisitionAndEnforcement() {
    StickyLeaseManager manager = new StickyLeaseManager(10, 10);
    NamespaceId ns1 = new NamespaceId("ns1");
    NamespaceId ns2 = new NamespaceId("ns2");

    // 1. Lease Acquisition (First-Write Wins)
    Assert.assertEquals(StickyLeaseManager.AcquisitionStatus.SUCCESS,
                        manager.acquireLease(ns1, StickyLeaseManager.TenantTier.BASIC));
    Assert.assertEquals(ns1, manager.getCurrentLease());

    // Same namespace can acquire / start tasks
    Assert.assertEquals(StickyLeaseManager.AcquisitionStatus.SUCCESS,
                        manager.startTask(ns1, StickyLeaseManager.TenantTier.BASIC));

    // 2. Enforcement (Mismatching namespace rejected -> maps to 429 TOO_MANY_REQUESTS)
    Assert.assertEquals(StickyLeaseManager.AcquisitionStatus.REJECTED_MISMATCH,
                        manager.acquireLease(ns2, StickyLeaseManager.TenantTier.BASIC));
    Assert.assertEquals(StickyLeaseManager.AcquisitionStatus.REJECTED_MISMATCH,
                        manager.startTask(ns2, StickyLeaseManager.TenantTier.BASIC));
  }

  @Test
  public void testMaxConcurrencyControl() {
    int maxConcurrency = 5;
    StickyLeaseManager manager = new StickyLeaseManager(maxConcurrency, 10);
    NamespaceId ns = new NamespaceId("tenant");

    for (int i = 0; i < maxConcurrency; i++) {
      Assert.assertEquals("Task " + (i + 1) + " should start",
                          StickyLeaseManager.AcquisitionStatus.SUCCESS,
                          manager.startTask(ns, StickyLeaseManager.TenantTier.ENTERPRISE));
    }

    // Exceeding concurrency limit rejected
    Assert.assertEquals(StickyLeaseManager.AcquisitionStatus.REJECTED_MAX_CONCURRENCY,
                        manager.startTask(ns, StickyLeaseManager.TenantTier.ENTERPRISE));
  }

  @Test
  public void testLogicalResetAfterTenTasks() {
    int maxTasksPerLease = 10;
    StickyLeaseManager manager = new StickyLeaseManager(10, maxTasksPerLease);
    NamespaceId ns = new NamespaceId("ns");

    for (int i = 0; i < maxTasksPerLease; i++) {
      Assert.assertEquals(StickyLeaseManager.AcquisitionStatus.SUCCESS,
                          manager.startTask(ns, StickyLeaseManager.TenantTier.BASIC));
      manager.finishTask(ns);
    }

    // Release Lease (Logical Reset): After processing 10 total tasks, pod clears namespace context
    Assert.assertNull("Lease should be logically reset after processing 10 tasks",
                      manager.getCurrentLease());
  }

  @Test
  public void testSwitchingDelaySoftReset() {
    StickyLeaseManager manager = new StickyLeaseManager(10, 2);
    NamespaceId ns1 = new NamespaceId("ns1");
    NamespaceId ns2 = new NamespaceId("ns2");

    // ns1 claims and finishes 2 tasks -> triggers soft reset
    manager.startTask(ns1, StickyLeaseManager.TenantTier.BASIC);
    manager.finishTask(ns1);
    manager.startTask(ns1, StickyLeaseManager.TenantTier.BASIC);
    manager.finishTask(ns1);

    Assert.assertNull(manager.getCurrentLease());

    // Switching Delay: soft reset allows new namespace ns2 to claim instantly (< 10ms) without 40s boot penalty
    long startClaim = System.currentTimeMillis();
    Assert.assertEquals(StickyLeaseManager.AcquisitionStatus.SUCCESS,
                        manager.acquireLease(ns2, StickyLeaseManager.TenantTier.DEVELOPER));
    long delay = System.currentTimeMillis() - startClaim;
    Assert.assertTrue("Switching delay should be < 10ms", delay < 10);
    Assert.assertEquals(ns2, manager.getCurrentLease());
  }

  @Test
  public void testTieredInactivityReclamation() {
    StickyLeaseManager manager = new StickyLeaseManager(10, 10);
    NamespaceId devNs = new NamespaceId("dev");

    manager.acquireLease(devNs, StickyLeaseManager.TenantTier.DEVELOPER);
    Assert.assertFalse(manager.enforceInactivityReclamation());

    // Simulate inactivity past Developer 5-second threshold
    manager.setLastActivityTimeMillis(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(6));
    Assert.assertTrue("Developer tier 5s inactivity timeout should trigger reclamation",
                      manager.enforceInactivityReclamation());
    Assert.assertNull(manager.getCurrentLease());
  }
}

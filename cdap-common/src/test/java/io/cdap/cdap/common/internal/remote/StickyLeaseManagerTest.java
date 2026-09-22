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

import io.cdap.cdap.common.internal.remote.StickyLeaseManager.AdmissionStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link StickyLeaseManager}, with particular attention to the credential lifetime: the
 * whole point of the class is that the namespaced credential is provisioned exactly once per burst
 * of overlapping tasks and wiped the instant the pod goes idle.
 */
public class StickyLeaseManagerTest {

  private static final NamespaceId NS1 = new NamespaceId("ns1");
  private static final NamespaceId NS2 = new NamespaceId("ns2");

  /**
   * Records credential provisioning and wiping so tests can assert on the exact sequence rather
   * than just the final state. Order matters here: a wipe landing after a provision for the same
   * burst would leave running tasks without an identity.
   */
  private static final class RecordingCredentialSink implements NamespaceCredentialContext {

    private final List<String> events = new ArrayList<>();
    private boolean provisioningFails;

    @Override
    public void provision(NamespaceId namespace) {
      if (provisioningFails) {
        // Nothing is recorded: a provision that threw did not happen as far as the pod is
        // concerned, and the tests assert exactly that.
        throw new IllegalStateException("metadata sidecar unreachable");
      }
      events.add("set:" + namespace.getNamespace());
    }

    @Override
    public void wipe() {
      events.add("clear");
    }

    private StickyLeaseManager newManager(int maxConcurrentTasks) {
      return new StickyLeaseManager(maxConcurrentTasks, this);
    }
  }

  @Test
  public void testFirstTaskClaimsLeaseAndProvisionsCredential() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    StickyLeaseManager manager = sink.newManager(10);

    Assert.assertEquals(AdmissionStatus.SUCCESS, manager.admitTask(NS1));

    Assert.assertEquals(NS1, manager.getCurrentLease());
    Assert.assertEquals(1, manager.getActiveTaskCount());
    Assert.assertEquals(java.util.Collections.singletonList("set:ns1"), sink.events);
  }

  @Test
  public void testOverlappingTasksOnSameNamespaceProvisionOnlyOnce() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    StickyLeaseManager manager = sink.newManager(10);

    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(AdmissionStatus.SUCCESS, manager.admitTask(NS1));
    }

    Assert.assertEquals(5, manager.getActiveTaskCount());
    Assert.assertEquals("Tasks after the first must reuse the credential the first one provisioned",
        java.util.Collections.singletonList("set:ns1"), sink.events);
  }

  @Test
  public void testCredentialSurvivesUntilTheLastTaskFinishes() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    StickyLeaseManager manager = sink.newManager(10);

    manager.admitTask(NS1);
    manager.admitTask(NS1);
    manager.admitTask(NS1);

    manager.releaseTask(NS1);
    manager.releaseTask(NS1);
    Assert.assertEquals("The credential must not be wiped while tasks are still running",
        java.util.Collections.singletonList("set:ns1"), sink.events);

    manager.releaseTask(NS1);
    Assert.assertEquals(java.util.Arrays.asList("set:ns1", "clear"), sink.events);
    Assert.assertEquals(0, manager.getActiveTaskCount());
  }

  @Test
  public void testRejectsOtherNamespaceWhileBusy() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    StickyLeaseManager manager = sink.newManager(10);

    manager.admitTask(NS1);

    Assert.assertEquals(AdmissionStatus.REJECTED_MISMATCH, manager.admitTask(NS2));
    Assert.assertEquals("A rejected task must not disturb the lease", NS1, manager.getCurrentLease());
    Assert.assertEquals(1, manager.getActiveTaskCount());
    Assert.assertEquals(java.util.Collections.singletonList("set:ns1"), sink.events);
  }

  @Test
  public void testRejectsOwnNamespaceAtConcurrencyLimit() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    StickyLeaseManager manager = sink.newManager(2);

    Assert.assertEquals(AdmissionStatus.SUCCESS, manager.admitTask(NS1));
    Assert.assertEquals(AdmissionStatus.SUCCESS, manager.admitTask(NS1));
    Assert.assertEquals(AdmissionStatus.REJECTED_MAX_CONCURRENCY, manager.admitTask(NS1));

    Assert.assertEquals(2, manager.getActiveTaskCount());
  }

  @Test
  public void testIdlePodIsHandedToAnotherNamespace() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    StickyLeaseManager manager = sink.newManager(10);

    manager.admitTask(NS1);
    manager.releaseTask(NS1);

    // The proxy steals idle pods without consulting how long they have been idle, so the worker
    // must hand one over on the same terms or it would reject traffic the proxy keeps sending.
    Assert.assertEquals(AdmissionStatus.SUCCESS, manager.admitTask(NS2));

    Assert.assertEquals(NS2, manager.getCurrentLease());
    Assert.assertEquals(java.util.Arrays.asList("set:ns1", "clear", "set:ns2"), sink.events);
  }

  @Test
  public void testReWakingTheSameNamespaceReprovisions() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    StickyLeaseManager manager = sink.newManager(10);

    manager.admitTask(NS1);
    manager.releaseTask(NS1);
    manager.admitTask(NS1);

    // Going idle wiped the credential, so the pod genuinely has none to reuse even though the
    // lease still names the same namespace.
    Assert.assertEquals(java.util.Arrays.asList("set:ns1", "clear", "set:ns1"), sink.events);
    Assert.assertEquals(1, manager.getActiveTaskCount());
  }

  @Test
  public void testCompletionWithoutMatchingAdmissionIsIgnored() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    StickyLeaseManager manager = sink.newManager(10);

    manager.releaseTask(NS1);

    Assert.assertEquals(0, manager.getActiveTaskCount());
    Assert.assertTrue("A stray completion must not wipe a credential that was never set",
        sink.events.isEmpty());
  }

  @Test
  public void testDuplicateCompletionDoesNotDriveCountNegative() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    StickyLeaseManager manager = sink.newManager(10);

    manager.admitTask(NS1);
    manager.releaseTask(NS1);
    manager.releaseTask(NS1);

    // A negative count would let the pod admit more tasks than the limit allows, and would wipe
    // the credential a second time underneath whoever claimed the pod next.
    Assert.assertEquals(0, manager.getActiveTaskCount());
    Assert.assertEquals(java.util.Arrays.asList("set:ns1", "clear"), sink.events);
  }

  @Test
  public void testCompletionForAnotherNamespaceIsIgnored() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    StickyLeaseManager manager = sink.newManager(10);

    manager.admitTask(NS1);
    manager.releaseTask(NS2);

    Assert.assertEquals(1, manager.getActiveTaskCount());
    Assert.assertEquals(java.util.Collections.singletonList("set:ns1"), sink.events);
  }

  @Test
  public void testFailedProvisioningLeavesNoLeaseBehind() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    sink.provisioningFails = true;
    StickyLeaseManager manager = sink.newManager(10);

    try {
      manager.admitTask(NS1);
      Assert.fail("Provisioning failure must propagate so the task is never run without identity");
    } catch (IllegalStateException e) {
      // expected
    }

    // Leaving the lease pointing at ns1 would advertise an identity the pod cannot actually
    // present, and would block ns2 from a pod that is doing nothing.
    Assert.assertNull(manager.getCurrentLease());
    Assert.assertEquals(0, manager.getActiveTaskCount());
  }

  @Test
  public void testLeaseIsReusableAfterFailedProvisioning() {
    RecordingCredentialSink sink = new RecordingCredentialSink();
    sink.provisioningFails = true;
    StickyLeaseManager manager = sink.newManager(10);

    try {
      manager.admitTask(NS1);
      Assert.fail("Expected provisioning to fail");
    } catch (IllegalStateException e) {
      // expected
    }

    sink.provisioningFails = false;
    Assert.assertEquals(AdmissionStatus.SUCCESS, manager.admitTask(NS2));
    Assert.assertEquals(NS2, manager.getCurrentLease());
    Assert.assertEquals(java.util.Collections.singletonList("set:ns2"), sink.events);
  }
}

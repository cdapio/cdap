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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proof of Concept (POC) Manager for Sticky Lease with Lifecycle & Reclamation.
 * <p>
 * Demonstrates: 1. "First-Write Wins" Lease Acquisition (pinning pod to a namespace). 2.
 * Enforcement: Rejection of mismatching namespace requests (429 TOO_MANY_REQUESTS). 3. Concurrency
 * Control: Up to 10 concurrent tasks. 4. Logical Reset & Reclamation: - Task-Count Reclamation
 * (releasing lease after 10 total tasks). - Tiered Inactivity Timeouts (Enterprise: 30s, Basic:
 * 10s, Developer: 5s). 5. Switching Delay: Instant soft reset in < 10ms avoiding 40s cold boot
 * penalty.
 */
public class StickyLeaseManager {

  private static final Logger LOG = LoggerFactory.getLogger(StickyLeaseManager.class);
  private final int maxConcurrentTasks;
  private final int maxTasksPerLease;
  private final AtomicReference<NamespaceId> currentLease = new AtomicReference<>(null);
  private final AtomicReference<TenantTier> currentTier = new AtomicReference<>(TenantTier.BASIC);
  private final AtomicInteger activeTaskCount = new AtomicInteger(0);
  private final AtomicInteger totalTasksProcessedInLease = new AtomicInteger(0);
  private volatile long lastActivityTimeMillis;
  private final Consumer<NamespaceId> onLeaseAcquired;
  private final Runnable onLeaseReleased;

  public StickyLeaseManager() {
    this(10, 10, null, null);
  }

  public StickyLeaseManager(int maxConcurrentTasks, int maxTasksPerLease,
                            Consumer<NamespaceId> onLeaseAcquired,
                            Runnable onLeaseReleased) {
    this.maxConcurrentTasks = maxConcurrentTasks;
    this.maxTasksPerLease = maxTasksPerLease;
    this.lastActivityTimeMillis = System.currentTimeMillis();
    this.onLeaseAcquired = onLeaseAcquired;
    this.onLeaseReleased = onLeaseReleased;
  }

  /**
   * Attempts to acquire or verify the sticky lease using "First-Write Wins" logic.
   */
  public synchronized AcquisitionStatus acquireLease(NamespaceId namespace, TenantTier tier) {
    NamespaceId existing = currentLease.get();

    if (existing == null) {
      // Idle pod claimed by new namespace in < 10ms (Soft Reset / Claim)
      long claimStartTime = System.currentTimeMillis();
      if (currentLease.compareAndSet(null, namespace)) {
        currentTier.set(tier);
        activeTaskCount.set(0);
        totalTasksProcessedInLease.set(0);
        lastActivityTimeMillis = System.currentTimeMillis();
        long elapsed = System.currentTimeMillis() - claimStartTime;
        LOG.info(
            "Lease claimed by namespace '{}' (Tier: {}) in {}ms (Boot penalty entirely avoided)",
            namespace.getNamespace(), tier, elapsed);
        if (onLeaseAcquired != null) {
          onLeaseAcquired.accept(namespace);
        }
        return AcquisitionStatus.SUCCESS;
      }
    }

    // Check if matching existing lease
    if (namespace.equals(currentLease.get())) {
      lastActivityTimeMillis = System.currentTimeMillis();
      return AcquisitionStatus.SUCCESS;
    }

    // Mismatching namespace -> Enforce rejection (triggering 429 TOO_MANY_REQUESTS / spillover)
    LOG.info("Enforcement: Rejecting request for namespace '{}', current lease is held by '{}'",
        namespace.getNamespace(), currentLease.get());
    return AcquisitionStatus.REJECTED_MISMATCH;
  }

  /**
   * Starts a task for the given namespace if lease and concurrency allow.
   */
  public synchronized AcquisitionStatus startTask(NamespaceId namespace, TenantTier tier) {
    AcquisitionStatus status = acquireLease(namespace, tier);
    if (status != AcquisitionStatus.SUCCESS) {
      return status;
    }

    if (activeTaskCount.get() >= maxConcurrentTasks) {
      LOG.info("Concurrency limit reached ({} tasks active) for namespace '{}'",
          activeTaskCount.get(), namespace.getNamespace());
      return AcquisitionStatus.REJECTED_MAX_CONCURRENCY;
    }

    activeTaskCount.incrementAndGet();
    lastActivityTimeMillis = System.currentTimeMillis();
    return AcquisitionStatus.SUCCESS;
  }

  /**
   * Finishes a task and evaluates Reclamation / Logical Reset after 10 total tasks.
   */
  public synchronized void finishTask(NamespaceId namespace) {
    if (namespace.equals(currentLease.get())) {
      activeTaskCount.decrementAndGet();
      lastActivityTimeMillis = System.currentTimeMillis();
      int completed = totalTasksProcessedInLease.incrementAndGet();

      // Reclamation: After processing 10 total tasks, release lease (Logical Reset)
      if (completed >= maxTasksPerLease && activeTaskCount.get() == 0) {
        releaseLease(
            "Processed " + completed + " total tasks (Max " + maxTasksPerLease + " reached)");
      }
    }
  }

  /**
   * Checks if the idle timeout for the current tiered tenancy has been exceeded. If exceeded,
   * triggers a logical reset.
   */
  public synchronized boolean enforceInactivityReclamation() {
    NamespaceId leased = currentLease.get();
    if (leased != null && activeTaskCount.get() == 0) {
      long idleDurationMillis = System.currentTimeMillis() - lastActivityTimeMillis;
      long threshold = currentTier.get().getInactivityTimeoutMillis();

      if (idleDurationMillis >= threshold) {
        releaseLease(String.format("Tiered inactivity timeout exceeded for %s (%dms >= %dms)",
            currentTier.get(), idleDurationMillis, threshold));
        return true;
      }
    }
    return false;
  }

  /**
   * Releases the lease (Logical Reset), clearing internal namespace context and sidecar state.
   */
  public synchronized void releaseLease(String reason) {
    NamespaceId oldNamespace = currentLease.getAndSet(null);
    if (oldNamespace != null) {
      activeTaskCount.set(0);
      totalTasksProcessedInLease.set(0);
      LOG.info("Release Lease (Logical Reset): Cleared namespace context for '{}'. Reason: {}",
          oldNamespace.getNamespace(), reason);
      if (onLeaseReleased != null) {
        onLeaseReleased.run();
      }
    }
  }

  @Nullable
  public NamespaceId getCurrentLease() {
    return currentLease.get();
  }

  public int getActiveTaskCount() {
    return activeTaskCount.get();
  }

  public int getTotalTasksProcessedInLease() {
    return totalTasksProcessedInLease.get();
  }

  public void setLastActivityTimeMillis(long timestampMillis) {
    this.lastActivityTimeMillis = timestampMillis;
  }

  public enum TenantTier {
    ENTERPRISE(TimeUnit.SECONDS.toMillis(30)),
    BASIC(TimeUnit.SECONDS.toMillis(10)),
    DEVELOPER(TimeUnit.SECONDS.toMillis(5));

    private final long inactivityTimeoutMillis;

    TenantTier(long inactivityTimeoutMillis) {
      this.inactivityTimeoutMillis = inactivityTimeoutMillis;
    }

    public long getInactivityTimeoutMillis() {
      return inactivityTimeoutMillis;
    }
  }

  public enum AcquisitionStatus {
    SUCCESS,
    REJECTED_MISMATCH,
    REJECTED_MAX_CONCURRENCY
  }
}

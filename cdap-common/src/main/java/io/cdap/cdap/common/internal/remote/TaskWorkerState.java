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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/** Lease and routing state of one task worker pod, updated lock-free through CAS. */
class TaskWorkerState {

  // A restart keeps the port closed for 12s to 88s (median 29s), so probe early and often.
  private static final long BACKOFF_BASE_NANOS = TimeUnit.SECONDS.toNanos(10);
  private static final long BACKOFF_MAX_NANOS = TimeUnit.SECONDS.toNanos(30);
  // Skip a pod that answered 429 so immediate retries try other pods before re-probing it.
  static final long REJECTION_SKIP_NANOS = TimeUnit.SECONDS.toNanos(5);

  /** Immutable snapshot of a pod's lease; every change swaps in a new instance via CAS. */
  static final class State {
    @Nullable
    final String leasedNamespace;
    final int inflightRequests;
    final long lastActivityTime;
    /** Failed connects or TERMINATING responses since the last successful connect. */
    final int failures;
    /** Clock timestamp in nanos until which the pod is skipped, if {@code failures > 0}. */
    final long unavailableUntil;
    /** Whether {@link #rejectedUntil} is set, since {@code 0L} is a valid nanoTime deadline. */
    final boolean hasRejectionDeadline;
    /** Clock timestamp in nanos until which the pod is skipped after a 429. */
    final long rejectedUntil;
    /** Whether the cooldown lets {@link #leasedNamespace} through, as the pod is not full. */
    final boolean cooldownExemptsHolder;

    private State(@Nullable String leasedNamespace, int inflightRequests, long lastActivityTime,
        int failures, long unavailableUntil, boolean hasRejectionDeadline, long rejectedUntil,
        boolean cooldownExemptsHolder) {
      this.leasedNamespace = leasedNamespace;
      this.inflightRequests = inflightRequests;
      this.lastActivityTime = lastActivityTime;
      this.failures = failures;
      this.unavailableUntil = unavailableUntil;
      this.hasRejectionDeadline = hasRejectionDeadline;
      this.rejectedUntil = rejectedUntil;
      this.cooldownExemptsHolder = cooldownExemptsHolder;
    }

    static State initial(@Nullable String leasedNamespace, int inflightRequests, long nowNanos) {
      return new State(leasedNamespace, inflightRequests, nowNanos, 0, 0L, false, 0L, false);
    }

    /** Returns a copy with new lease fields and the same backoff and rejection cooldown. */
    State withLease(@Nullable String leasedNamespace, int inflightRequests, long lastActivityTime) {
      return new State(leasedNamespace, inflightRequests, lastActivityTime, failures,
          unavailableUntil, hasRejectionDeadline, rejectedUntil, cooldownExemptsHolder);
    }

    State withBackoff(@Nullable String leasedNamespace, int inflightRequests,
        long lastActivityTime, int failures, long unavailableUntil) {
      return new State(leasedNamespace, inflightRequests, lastActivityTime, failures,
          unavailableUntil, hasRejectionDeadline, rejectedUntil, cooldownExemptsHolder);
    }

    State withoutBackoff() {
      return new State(leasedNamespace, inflightRequests, lastActivityTime, 0, 0L,
          hasRejectionDeadline, rejectedUntil, cooldownExemptsHolder);
    }

    State withRejection(@Nullable String leasedNamespace, int inflightRequests,
        long lastActivityTime, long rejectedUntil, boolean cooldownExemptsHolder) {
      return new State(leasedNamespace, inflightRequests, lastActivityTime, failures,
          unavailableUntil, true, rejectedUntil, cooldownExemptsHolder);
    }

    boolean isBackingOffAt(long nowNanos) {
      return failures > 0 && nowNanos - unavailableUntil < 0;
    }

    /** Returns whether the rejection cooldown hides the pod from {@code namespace}. */
    boolean isCoolingDownFor(@Nullable String namespace, long nowNanos) {
      if (!hasRejectionDeadline || nowNanos - rejectedUntil >= 0) {
        return false;
      }
      return !cooldownExemptsHolder || namespace == null || !namespace.equals(leasedNamespace);
    }

    /** Returns whether {@code namespace} can be routed to the pod at {@code nowNanos}. */
    boolean isAvailableFor(@Nullable String namespace, long nowNanos) {
      return !isBackingOffAt(nowNanos) && !isCoolingDownFor(namespace, nowNanos);
    }
  }

  private final AtomicReference<State> stateRef;
  @Nullable
  private final String nodeName;

  /**
   * Creates the lease state for a pod.
   *
   * @param leasedNamespace the namespace the pod is leased to, or {@code null} if unleased
   * @param inflightRequests requests this manager has routed to the pod and not yet released
   * @param nodeName the Kubernetes node running the pod, or {@code null} if unknown
   * @param nowNanos initial activity timestamp in nanoseconds
   */
  TaskWorkerState(@Nullable String leasedNamespace, int inflightRequests, @Nullable String nodeName,
      long nowNanos) {
    this.stateRef =
        new AtomicReference<>(State.initial(leasedNamespace, inflightRequests, nowNanos));
    this.nodeName = nodeName;
  }

  /** Returns the Kubernetes node running the pod, or {@code null} if unknown. */
  @Nullable
  String getNodeName() {
    return nodeName;
  }

  /** Returns an atomic point-in-time snapshot of the pod's state. */
  State snapshot() {
    return stateRef.get();
  }

  /** Returns the namespace the pod is leased to, or {@code null} if it has never been leased. */
  @Nullable
  String getLeasedNamespace() {
    return stateRef.get().leasedNamespace;
  }

  /** Returns the number of requests this manager has routed to the pod and not yet released. */
  int getInflightRequests() {
    return stateRef.get().inflightRequests;
  }

  /**
   * Returns the clock timestamp in nanos of the last state change, used to pick idle pods to
   * steal.
   */
  long getLastActivityTime() {
    return stateRef.get().lastActivityTime;
  }

  /**
   * Takes a slot if the pod is available to {@code namespace}, already leased to it, and below
   * {@code maxConcurrency}.
   *
   * @return {@code true} if a slot was taken
   */
  boolean tryAcquireWarmLease(String namespace, int maxConcurrency, long nowNanos) {
    while (true) {
      State current = stateRef.get();
      if (!current.isAvailableFor(namespace, nowNanos)
          || !namespace.equals(current.leasedNamespace)
          || current.inflightRequests >= maxConcurrency) {
        return false;
      }
      State next =
          current.withLease(current.leasedNamespace, current.inflightRequests + 1, nowNanos);
      if (stateRef.compareAndSet(current, next)) {
        return true;
      }
    }
  }

  /**
   * Leases a never-leased, idle pod to {@code namespace} and takes a slot, or joins a claim that
   * another request for {@code namespace} just made.
   *
   * @return {@code true} if a slot was taken
   */
  boolean tryClaimFreshLease(String namespace, int maxConcurrency, long nowNanos) {
    while (true) {
      State current = stateRef.get();
      if (!current.isAvailableFor(namespace, nowNanos)) {
        return false;
      }
      boolean isUnleased = current.leasedNamespace == null || current.leasedNamespace.isEmpty();
      boolean isFresh = isUnleased && current.inflightRequests == 0;
      boolean isSameNamespaceWithRoom =
          namespace.equals(current.leasedNamespace) && current.inflightRequests < maxConcurrency;
      if (!isFresh && !isSameNamespaceWithRoom) {
        return false;
      }
      State next = current.withLease(namespace, current.inflightRequests + 1, nowNanos);
      if (stateRef.compareAndSet(current, next)) {
        return true;
      }
    }
  }

  /**
   * Re-leases an idle pod to {@code namespace} and takes a slot, or joins a steal that another
   * request for {@code namespace} just made.
   *
   * @return {@code true} if a slot was taken
   */
  boolean tryStealIdleLease(String namespace, int maxConcurrency, long nowNanos) {
    while (true) {
      State current = stateRef.get();
      if (!current.isAvailableFor(namespace, nowNanos)) {
        return false;
      }
      boolean isIdle = current.inflightRequests == 0;
      boolean isSameNamespaceWithRoom =
          namespace.equals(current.leasedNamespace) && current.inflightRequests < maxConcurrency;
      if (!isIdle && !isSameNamespaceWithRoom) {
        return false;
      }
      State next = current.withLease(namespace, current.inflightRequests + 1, nowNanos);
      if (stateRef.compareAndSet(current, next)) {
        return true;
      }
    }
  }

  /** Releases one slot. The count never drops below zero, so late releases are harmless. */
  void decrementInflightRequests(long nowNanos) {
    while (true) {
      State current = stateRef.get();
      State next = current.withLease(
          current.leasedNamespace, Math.max(0, current.inflightRequests - 1), nowNanos);
      if (stateRef.compareAndSet(current, next)) {
        return;
      }
    }
  }

  /**
   * Handles a worker's 429: releases this request's slot, adopts the namespace the worker reports
   * and cools the pod down, except for a lease holder other than {@code requestNamespace}.
   *
   * @param leasedNamespace the namespace the worker reports, or {@code null} if absent
   * @param requestNamespace the namespace of the rejected request
   * @param nowNanos current clock timestamp in nanoseconds
   * @return the updated {@link State} installed by the CAS
   */
  State markRejected(@Nullable String leasedNamespace, String requestNamespace, long nowNanos) {
    // A worker that names the requester as its holder is full; any other holder may have room.
    boolean namesOtherHolder = leasedNamespace != null && !leasedNamespace.isEmpty()
        && !leasedNamespace.equals(requestNamespace);
    while (true) {
      State current = stateRef.get();
      int nextInflight = Math.max(0, current.inflightRequests - 1);
      String nextNamespace =
          resolveRejectedNamespace(current, leasedNamespace, nextInflight, nowNanos);
      // Nobody is exempt while the pod backs off or a cooldown already keeps its holder out.
      boolean exemptsHolder = namesOtherHolder && leasedNamespace.equals(nextNamespace)
          && !current.isCoolingDownFor(current.leasedNamespace, nowNanos);
      State next = current.withRejection(
          nextNamespace, nextInflight, nowNanos, nowNanos + REJECTION_SKIP_NANOS, exemptsHolder);
      if (stateRef.compareAndSet(current, next)) {
        return next;
      }
    }
  }

  @Nullable
  private static String resolveRejectedNamespace(State current, @Nullable String leasedNamespace,
      int nextInflight, long nowNanos) {
    if (current.isBackingOffAt(nowNanos)) {
      return null;
    }
    if (leasedNamespace != null && !leasedNamespace.isEmpty()) {
      return leasedNamespace;
    }
    if (nextInflight > 0) {
      return current.leasedNamespace;
    }
    return null;
  }

  /** Marks the pod as recently used, so it is less likely to be stolen. */
  void recordActivity(long nowNanos) {
    while (true) {
      State current = stateRef.get();
      State next = current.withLease(current.leasedNamespace, current.inflightRequests, nowNanos);
      if (stateRef.compareAndSet(current, next)) {
        return;
      }
    }
  }

  /**
   * Returns whether the pod can take requests from any namespace at {@code nowNanos}, i.e. it is
   * neither backing off nor cooling down after a rejection.
   */
  boolean isAvailable(long nowNanos) {
    return stateRef.get().isAvailableFor(null, nowNanos);
  }

  /**
   * Handles a refused connection or a terminating pod: releases this request's slot, drops the
   * lease and backs the pod off. Failures during a backoff don't extend it.
   *
   * @return the new backoff in nanoseconds, or 0 if the pod was already backing off
   */
  long markUnavailable(long nowNanos) {
    while (true) {
      State current = stateRef.get();
      int inflight = Math.max(0, current.inflightRequests - 1);
      if (current.isBackingOffAt(nowNanos)) {
        State next = current.withBackoff(
            null, inflight, current.lastActivityTime, current.failures, current.unavailableUntil);
        if (stateRef.compareAndSet(current, next)) {
          return 0L;
        }
        continue;
      }
      int failures = current.failures + 1;
      long backoff = backoffNanos(failures);
      State next = current.withBackoff(null, inflight, nowNanos, failures, nowNanos + backoff);
      if (stateRef.compareAndSet(current, next)) {
        return backoff;
      }
    }
  }

  /** Clears the backoff once a connection to the pod succeeds. */
  void markReachable() {
    while (true) {
      State current = stateRef.get();
      if (current.failures == 0) {
        return;
      }
      State next = current.withoutBackoff();
      if (stateRef.compareAndSet(current, next)) {
        return;
      }
    }
  }

  /** Returns the backoff after {@code failures} consecutive failures: 10s, 20s, then 30s. */
  static long backoffNanos(int failures) {
    // Clamp the shift so a long outage can't overflow it.
    int shift = Math.min(Math.max(failures, 1) - 1, 2);
    return Math.min(BACKOFF_BASE_NANOS << shift, BACKOFF_MAX_NANOS);
  }
}

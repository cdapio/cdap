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

package io.cdap.cdap.internal.app.worker.manager;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

/** Lease and routing status of one task worker, updated lock-free through CAS. */
class TaskWorkerStatus {

  private static final long BACKOFF_BASE_NANOS = TimeUnit.SECONDS.toNanos(10);
  private static final long BACKOFF_MAX_NANOS = TimeUnit.SECONDS.toNanos(30);
  // Skip a worker that answered 429 so immediate retries try other workers before re-probing it.
  static final long REJECTION_SKIP_NANOS = TimeUnit.SECONDS.toNanos(5);

  private final AtomicReference<TaskWorkerStatusSnapshot> stateRef;
  @Nullable
  private final String nodeName;

  /**
   * Creates the status of a task worker.
   *
   * @param leasedNamespace the namespace the worker is leased to, or {@code null} if unleased
   * @param inflightRequests requests this manager has routed to the worker and not yet released
   * @param nodeName the node (host) the worker runs on, used to spread leases across nodes, or
   *     {@code null} if unknown
   * @param nowNanos initial activity timestamp in nanoseconds
   */
  TaskWorkerStatus(@Nullable String leasedNamespace, int inflightRequests, @Nullable String nodeName,
      long nowNanos) {
    this.stateRef = new AtomicReference<>(
        TaskWorkerStatusSnapshot.initial(leasedNamespace, inflightRequests, nowNanos));
    this.nodeName = nodeName;
  }

  /** Returns the node (host) the worker runs on, or {@code null} if unknown. */
  @Nullable
  String getNodeName() {
    return nodeName;
  }

  /** Returns an atomic point-in-time snapshot of the worker's status. */
  TaskWorkerStatusSnapshot snapshot() {
    return stateRef.get();
  }

  /** Returns the namespace the worker is leased to, or {@code null} if it has never been leased. */
  @Nullable
  String getLeasedNamespace() {
    return stateRef.get().leasedNamespace;
  }

  /** Returns the number of requests this manager has routed to the worker and not yet released. */
  int getInflightRequests() {
    return stateRef.get().inflightRequests;
  }

  /**
   * Takes a slot if the worker is available to {@code namespace}, already leased to it, and below
   * {@code maxConcurrency}.
   *
   * @return {@code true} if a slot was taken
   */
  boolean tryAcquireWarmLease(String namespace, int maxConcurrency, long nowNanos) {
    return update(current -> current.isAvailableFor(namespace, nowNanos)
        && namespace.equals(current.leasedNamespace)
        && current.inflightRequests < maxConcurrency
        ? current.withLease(current.leasedNamespace, current.inflightRequests + 1, nowNanos)
        : null) != null;
  }

  /**
   * Leases a never-leased, idle worker to {@code namespace} and takes a slot, or joins a claim that
   * another request for {@code namespace} just made.
   *
   * @return {@code true} if a slot was taken
   */
  boolean tryClaimFreshLease(String namespace, int maxConcurrency, long nowNanos) {
    return update(current -> {
      boolean isUnleased = current.leasedNamespace == null || current.leasedNamespace.isEmpty();
      boolean isFresh = isUnleased && current.inflightRequests == 0;
      return current.isAvailableFor(namespace, nowNanos)
          && (isFresh || hasRoomFor(current, namespace, maxConcurrency))
          ? current.withLease(namespace, current.inflightRequests + 1, nowNanos)
          : null;
    }) != null;
  }

  /**
   * Re-leases an idle worker to {@code namespace} and takes a slot, or joins a steal that another
   * request for {@code namespace} just made.
   *
   * @return {@code true} if a slot was taken
   */
  boolean tryStealIdleLease(String namespace, int maxConcurrency, long nowNanos) {
    return update(current -> current.isAvailableFor(namespace, nowNanos)
        && (current.inflightRequests == 0 || hasRoomFor(current, namespace, maxConcurrency))
        ? current.withLease(namespace, current.inflightRequests + 1, nowNanos)
        : null) != null;
  }

  /** Releases one slot. The count never drops below zero, so late releases are harmless. */
  void decrementInflightRequests(long nowNanos) {
    update(current -> current.withLease(
        current.leasedNamespace, Math.max(0, current.inflightRequests - 1), nowNanos));
  }

  /**
   * Handles a worker's 429: releases this request's slot, adopts the namespace the worker reports
   * and cools the worker down, except for a lease holder other than {@code requestNamespace}.
   *
   * @param leasedNamespace the namespace the worker reports, or {@code null} if absent
   * @param requestNamespace the namespace of the rejected request
   * @param nowNanos current clock timestamp in nanoseconds
   * @return the updated {@link TaskWorkerStatusSnapshot} installed by the CAS
   */
  TaskWorkerStatusSnapshot markRejected(@Nullable String leasedNamespace, String requestNamespace,
      long nowNanos) {
    // A worker that names the requester as its holder is full; any other holder may have room.
    boolean namesOtherHolder = leasedNamespace != null && !leasedNamespace.isEmpty()
        && !leasedNamespace.equals(requestNamespace);
    return update(current -> {
      int nextInflight = Math.max(0, current.inflightRequests - 1);
      String nextNamespace =
          resolveRejectedNamespace(current, leasedNamespace, nextInflight, nowNanos);
      // Nobody is exempt while the worker backs off or a cooldown already keeps its holder out.
      boolean exemptsHolder = namesOtherHolder && leasedNamespace.equals(nextNamespace)
          && !current.isCoolingDownFor(current.leasedNamespace, nowNanos);
      return current.withRejection(
          nextNamespace, nextInflight, nowNanos, nowNanos + REJECTION_SKIP_NANOS, exemptsHolder);
    });
  }

  @Nullable
  private static String resolveRejectedNamespace(TaskWorkerStatusSnapshot current,
      @Nullable String leasedNamespace, int nextInflight, long nowNanos) {
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

  /** Marks the worker as recently used, so it is less likely to be stolen. */
  void recordActivity(long nowNanos) {
    update(current ->
        current.withLease(current.leasedNamespace, current.inflightRequests, nowNanos));
  }

  /**
   * Handles a refused connection or a terminating worker: releases this request's slot, drops the
   * lease and backs the worker off. Failures during a backoff don't extend it.
   *
   * @return the new backoff in nanoseconds, or 0 if the worker was already backing off
   */
  long markUnavailable(long nowNanos) {
    while (true) {
      TaskWorkerStatusSnapshot current = stateRef.get();
      int inflight = Math.max(0, current.inflightRequests - 1);
      if (current.isBackingOffAt(nowNanos)) {
        TaskWorkerStatusSnapshot next = current.withBackoff(
            null, inflight, current.lastActivityTime, current.failures, current.unavailableUntil);
        if (stateRef.compareAndSet(current, next)) {
          return 0L;
        }
        continue;
      }
      int failures = current.failures + 1;
      long backoff = backoffNanos(failures);
      TaskWorkerStatusSnapshot next =
          current.withBackoff(null, inflight, nowNanos, failures, nowNanos + backoff);
      if (stateRef.compareAndSet(current, next)) {
        return backoff;
      }
    }
  }

  /** Clears the backoff once a connection to the worker succeeds. */
  void markReachable() {
    update(current -> current.failures == 0 ? null : current.withoutBackoff());
  }

  /** Returns the backoff after {@code failures} consecutive failures: 10s, 20s, then 30s. */
  static long backoffNanos(int failures) {
    // Clamp the shift so a long outage can't overflow it.
    int shift = Math.min(Math.max(failures, 1) - 1, 2);
    return Math.min(BACKOFF_BASE_NANOS << shift, BACKOFF_MAX_NANOS);
  }

  private static boolean hasRoomFor(TaskWorkerStatusSnapshot current, String namespace,
      int maxConcurrency) {
    return namespace.equals(current.leasedNamespace) && current.inflightRequests < maxConcurrency;
  }

  /**
   * Applies {@code transition} to the current snapshot and installs the result through CAS,
   * retrying on contention.
   *
   * @return the installed snapshot, or {@code null} if {@code transition} returned {@code null},
   *     which leaves the status unchanged
   */
  @Nullable
  private TaskWorkerStatusSnapshot update(
      UnaryOperator<TaskWorkerStatusSnapshot> transition) {
    while (true) {
      TaskWorkerStatusSnapshot current = stateRef.get();
      TaskWorkerStatusSnapshot next = transition.apply(current);
      if (next == null || stateRef.compareAndSet(current, next)) {
        return next;
      }
    }
  }
}

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

import javax.annotation.Nullable;

/**
 * Immutable snapshot of a task worker's lease and routing state. {@link TaskWorkerStatus} swaps in
 * a new instance through CAS on every change.
 */
final class TaskWorkerStatusSnapshot {
  @Nullable
  final String leasedNamespace;
  final int inflightRequests;
  /** Clock timestamp in nanos of the last state change, used to pick idle workers to steal. */
  final long lastActivityTime;
  /** Failed connects or TERMINATING responses since the last successful connect. */
  final int failures;
  /** Clock timestamp in nanos until which the worker is skipped, if {@code failures > 0}. */
  final long unavailableUntil;
  /** Whether {@link #rejectedUntil} is set, since {@code 0L} is a valid nanoTime deadline. */
  final boolean hasRejectionDeadline;
  /** Clock timestamp in nanos until which the worker is skipped after a 429. */
  final long rejectedUntil;
  /** Whether the cooldown lets {@link #leasedNamespace} through, as the worker is not full. */
  final boolean cooldownExemptsHolder;

  private TaskWorkerStatusSnapshot(@Nullable String leasedNamespace, int inflightRequests,
      long lastActivityTime, int failures, long unavailableUntil, boolean hasRejectionDeadline,
      long rejectedUntil, boolean cooldownExemptsHolder) {
    this.leasedNamespace = leasedNamespace;
    this.inflightRequests = inflightRequests;
    this.lastActivityTime = lastActivityTime;
    this.failures = failures;
    this.unavailableUntil = unavailableUntil;
    this.hasRejectionDeadline = hasRejectionDeadline;
    this.rejectedUntil = rejectedUntil;
    this.cooldownExemptsHolder = cooldownExemptsHolder;
  }

  static TaskWorkerStatusSnapshot initial(@Nullable String leasedNamespace, int inflightRequests,
      long nowNanos) {
    return new TaskWorkerStatusSnapshot(leasedNamespace, inflightRequests, nowNanos, 0, 0L, false,
        0L, false);
  }

  /** Returns a copy with new lease fields and the same backoff and rejection cooldown. */
  TaskWorkerStatusSnapshot withLease(@Nullable String leasedNamespace, int inflightRequests,
      long lastActivityTime) {
    return new TaskWorkerStatusSnapshot(leasedNamespace, inflightRequests, lastActivityTime,
        failures, unavailableUntil, hasRejectionDeadline, rejectedUntil, cooldownExemptsHolder);
  }

  TaskWorkerStatusSnapshot withBackoff(@Nullable String leasedNamespace, int inflightRequests,
      long lastActivityTime, int failures, long unavailableUntil) {
    return new TaskWorkerStatusSnapshot(leasedNamespace, inflightRequests, lastActivityTime,
        failures, unavailableUntil, hasRejectionDeadline, rejectedUntil, cooldownExemptsHolder);
  }

  TaskWorkerStatusSnapshot withoutBackoff() {
    return new TaskWorkerStatusSnapshot(leasedNamespace, inflightRequests, lastActivityTime, 0, 0L,
        hasRejectionDeadline, rejectedUntil, cooldownExemptsHolder);
  }

  TaskWorkerStatusSnapshot withRejection(@Nullable String leasedNamespace, int inflightRequests,
      long lastActivityTime, long rejectedUntil, boolean cooldownExemptsHolder) {
    return new TaskWorkerStatusSnapshot(leasedNamespace, inflightRequests, lastActivityTime,
        failures, unavailableUntil, true, rejectedUntil, cooldownExemptsHolder);
  }

  boolean isBackingOffAt(long nowNanos) {
    return failures > 0 && nowNanos - unavailableUntil < 0;
  }

  /** Returns whether the rejection cooldown hides the worker from {@code namespace}. */
  boolean isCoolingDownFor(@Nullable String namespace, long nowNanos) {
    if (!hasRejectionDeadline || nowNanos - rejectedUntil >= 0) {
      return false;
    }
    return !cooldownExemptsHolder || namespace == null || !namespace.equals(leasedNamespace);
  }

  /**
   * Returns whether {@code namespace} can be routed to the worker at {@code nowNanos}. A
   * {@code null} namespace asks whether any namespace can.
   */
  boolean isAvailableFor(@Nullable String namespace, long nowNanos) {
    return !isBackingOffAt(nowNanos) && !isCoolingDownFor(namespace, nowNanos);
  }
}

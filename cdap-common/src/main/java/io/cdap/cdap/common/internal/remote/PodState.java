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

/**
 * PodState represents the in-memory routing and lease status of an individual Task Worker pod.
 * Entirely lock-free, backing state via an immutable internal representation and AtomicReference CAS loops.
 */
class PodState {

    // A restart keeps the port closed for 12s to 88s (median 29s), so probe early and often.
    private static final long BACKOFF_BASE_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long BACKOFF_MAX_NANOS = TimeUnit.SECONDS.toNanos(30);

    /** Immutable snapshot of a pod's lease; every change swaps in a new instance via CAS. */
    private static class State {
        final String leasedNamespace;
        final int inflightRequests;
        final long lastActivityTime;
        /** Failed connects or drain rejections since the last successful connect. */
        final int failures;
        /** {@link System#nanoTime()} until which the pod is skipped, if {@code failures > 0}. */
        final long unavailableUntil;

        State(String leasedNamespace, int inflightRequests, long lastActivityTime) {
            this(leasedNamespace, inflightRequests, lastActivityTime, 0, 0L);
        }

        State(String leasedNamespace, int inflightRequests, long lastActivityTime, int failures,
              long unavailableUntil) {
            this.leasedNamespace = leasedNamespace;
            this.inflightRequests = inflightRequests;
            this.lastActivityTime = lastActivityTime;
            this.failures = failures;
            this.unavailableUntil = unavailableUntil;
        }

        /** Returns a copy with new lease fields and the same backoff. */
        State withLease(String leasedNamespace, int inflightRequests, long lastActivityTime) {
            return new State(leasedNamespace, inflightRequests, lastActivityTime, failures, unavailableUntil);
        }
    }

    private final AtomicReference<State> stateRef;
    @Nullable
    private final String nodeName;

    /**
     * Creates the lease state for a pod on an unknown node.
     *
     * @param leasedNamespace the namespace the pod is leased to, or {@code null} if unleased
     * @param inflightRequests requests this proxy has routed to the pod and not yet released
     */
    PodState(String leasedNamespace, int inflightRequests) {
        this(leasedNamespace, inflightRequests, null);
    }

    /**
     * Creates the lease state for a pod.
     *
     * @param leasedNamespace the namespace the pod is leased to, or {@code null} if unleased
     * @param inflightRequests requests this proxy has routed to the pod and not yet released
     * @param nodeName the Kubernetes node running the pod, or {@code null} if unknown
     */
    PodState(String leasedNamespace, int inflightRequests, @Nullable String nodeName) {
        this.stateRef = new AtomicReference<>(new State(
            leasedNamespace,
            inflightRequests,
            System.nanoTime()
        ));
        this.nodeName = nodeName;
    }

    /** Returns the Kubernetes node running the pod, or {@code null} if unknown. */
    @Nullable
    String getNodeName() {
        return nodeName;
    }

    /** Returns the namespace the pod is leased to, or {@code null} if it has never been leased. */
    String getLeasedNamespace() {
        return stateRef.get().leasedNamespace;
    }

    /** Returns the number of requests this proxy has routed to the pod and not yet released. */
    int getInflightRequests() {
        return stateRef.get().inflightRequests;
    }

    /** Returns the {@link System#nanoTime()} of the last state change, used to pick idle pods to steal. */
    long getLastActivityTime() {
        return stateRef.get().lastActivityTime;
    }

    /**
     * Takes a slot if the pod is already leased to {@code namespace} and below {@code maxConcurrency}.
     *
     * @return {@code true} if a slot was taken
     */
    boolean tryAcquireWarmLease(String namespace, int maxConcurrency) {
        while (true) {
            State current = stateRef.get();
            if (!namespace.equals(current.leasedNamespace) || current.inflightRequests >= maxConcurrency) {
                return false;
            }
            State next = current.withLease(current.leasedNamespace, current.inflightRequests + 1, System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    /**
     * Leases a never-leased, idle pod to {@code namespace} and takes its first slot.
     *
     * @return {@code true} if the pod was claimed
     */
    boolean tryClaimFreshLease(String namespace) {
        while (true) {
            State current = stateRef.get();
            // Fresh pod: must have NO namespace and 0 inflight
            boolean isUnleased = current.leasedNamespace == null || current.leasedNamespace.isEmpty();
            if (!isUnleased || current.inflightRequests != 0) {
                return false;
            }
            State next = current.withLease(namespace, 1, System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    /**
     * Re-leases an idle pod to {@code namespace}, whoever held it before, and takes its first slot.
     *
     * @return {@code true} if the pod was taken over
     */
    boolean tryStealIdleLease(String namespace) {
        while (true) {
            State current = stateRef.get();
            // Stealable pod: ANY pod with 0 inflight requests
            if (current.inflightRequests != 0) {
                return false;
            }
            State next = current.withLease(namespace, 1, System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    /** Releases one slot. The count never drops below zero, so late releases are harmless. */
    void decrementInflightRequests() {
        while (true) {
            State current = stateRef.get();
            State next = current.withLease(current.leasedNamespace, 
                Math.max(0, current.inflightRequests - 1), System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }

    /**
     * Handles a worker rejection: releases this request's slot and adopts the worker's reported
     * namespace.
     *
     * @param leasedNamespace the namespace the worker reports, or {@code null} to keep the current one
     */
    void adoptRejectedLease(String leasedNamespace) {
        while (true) {
            State current = stateRef.get();
            String nextNamespace = leasedNamespace != null ? leasedNamespace : current.leasedNamespace;
            State next = current.withLease(nextNamespace, Math.max(0, current.inflightRequests - 1),
                System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }
    
    /** Marks the pod as recently used, so it is less likely to be stolen. */
    void recordActivity() {
        while (true) {
            State current = stateRef.get();
            State next = current.withLease(current.leasedNamespace, current.inflightRequests, System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }

    /** Returns whether the pod can take requests at {@code nowNanos}, i.e. it is not backing off. */
    boolean isAvailable(long nowNanos) {
        State current = stateRef.get();
        return current.failures == 0 || nowNanos - current.unavailableUntil >= 0;
    }

    /**
     * Handles a pod that refused a connection or is draining to restart: releases this request's
     * slot and skips the pod until a backoff expires. The next request after that probes it. The
     * namespace is dropped, since the pod comes back as a new JVM without a lease. Failures while
     * the pod is already backing off come from requests routed before it went down, so they don't
     * extend the backoff.
     *
     * @return the new backoff in nanoseconds, or 0 if the pod was already backing off
     */
    long markUnavailable(long nowNanos) {
        while (true) {
            State current = stateRef.get();
            int inflight = Math.max(0, current.inflightRequests - 1);
            if (current.failures > 0 && nowNanos - current.unavailableUntil < 0) {
                State next = new State(current.leasedNamespace, inflight, current.lastActivityTime,
                    current.failures, current.unavailableUntil);
                if (stateRef.compareAndSet(current, next)) {
                    return 0L;
                }
                continue;
            }
            int failures = current.failures + 1;
            long backoff = backoffNanos(failures);
            State next = new State(null, inflight, nowNanos, failures, nowNanos + backoff);
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
            State next = new State(current.leasedNamespace, current.inflightRequests, current.lastActivityTime,
                0, 0L);
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

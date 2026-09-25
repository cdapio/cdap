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

import java.util.concurrent.atomic.AtomicReference;

/**
 * PodState represents the in-memory routing and lease status of an individual Task Worker pod.
 * Entirely lock-free, backing state via an immutable internal representation and AtomicReference CAS loops.
 */
class PodState {
    /** Immutable snapshot of a pod's lease; every change swaps in a new instance via CAS. */
    private static class State {
        final String leasedNamespace;
        final int inflightRequests;
        final long lastActivityTime;

        State(String leasedNamespace, int inflightRequests, long lastActivityTime) {
            this.leasedNamespace = leasedNamespace;
            this.inflightRequests = inflightRequests;
            this.lastActivityTime = lastActivityTime;
        }
    }

    private final AtomicReference<State> stateRef;

    /**
     * Creates the lease state for a pod.
     *
     * @param leasedNamespace the namespace the pod is leased to, or {@code null} if unleased
     * @param inflightRequests requests this proxy has routed to the pod and not yet released
     */
    PodState(String leasedNamespace, int inflightRequests) {
        this.stateRef = new AtomicReference<>(new State(
            leasedNamespace,
            inflightRequests,
            System.nanoTime()
        ));
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
            State next = new State(current.leasedNamespace, current.inflightRequests + 1, System.nanoTime());
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
            State next = new State(namespace, 1, System.nanoTime());
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
            State next = new State(namespace, 1, System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    /** Releases one slot. The count never drops below zero, so late releases are harmless. */
    void decrementInflightRequests() {
        while (true) {
            State current = stateRef.get();
            State next = new State(current.leasedNamespace, 
                Math.max(0, current.inflightRequests - 1), System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }

    /**
     * Handles a worker rejection: releases this request's slot and adopts the worker's reported
     * namespace. The worker's task count is not copied, since those tasks aren't this proxy's to
     * release.
     *
     * @param leasedNamespace the namespace the worker reports, or {@code null} to keep the current one
     */
    void adoptRejectedLease(String leasedNamespace) {
        while (true) {
            State current = stateRef.get();
            String nextNamespace = leasedNamespace != null ? leasedNamespace : current.leasedNamespace;
            State next = new State(nextNamespace, Math.max(0, current.inflightRequests - 1),
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
            State next = new State(current.leasedNamespace, current.inflightRequests, System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }
}

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
 *
 * <p>A PodState is either listed or unlisted, mirroring whether the most recent discovery sync
 * reported the pod. An unlisted pod stays in the registry only so that the requests still running
 * on it can release their slots; it is never offered for a new lease. Unlisting and every lease
 * acquisition are CAS transitions on the same reference, so once {@link #setListed(boolean)}
 * unlists a pod, no lease can be added to it until it is listed again.
 */
class PodState {
    private static class State {
        final String leasedNamespace;
        final int inflightRequests;
        final long lastActivityTime;
        final boolean listed;

        State(String leasedNamespace, int inflightRequests, long lastActivityTime, boolean listed) {
            this.leasedNamespace = leasedNamespace;
            this.inflightRequests = inflightRequests;
            this.lastActivityTime = lastActivityTime;
            this.listed = listed;
        }
    }

    private final AtomicReference<State> stateRef;

    PodState(String leasedNamespace, int inflightRequests) {
        this.stateRef = new AtomicReference<>(new State(
            leasedNamespace,
            inflightRequests,
            System.nanoTime(),
            true
        ));
    }

    String getLeasedNamespace() {
        return stateRef.get().leasedNamespace;
    }

    int getInflightRequests() {
        return stateRef.get().inflightRequests;
    }

    long getLastActivityTime() {
        return stateRef.get().lastActivityTime;
    }

    boolean isListed() {
        return stateRef.get().listed;
    }

    /**
     * Records whether the latest discovery sync reported this pod.
     *
     * <p>A pod drops out of discovery when its container restarts in place, when it starts
     * terminating, or when its node is marked NotReady. Its entry cannot be removed while requests
     * are still in flight on it, because their releases would then find nothing to decrement. But
     * it must not attract new requests either: a warm match on a pod that is gone costs the caller
     * a connect timeout followed by a 502 and a retry, and if the node vanished without closing
     * its sockets the entry can take up to the caller's read timeout to drain. Unlisting keeps the
     * entry for accounting while taking it out of every lease path. If discovery reports the pod
     * again it is simply relisted with its counts intact.
     */
    void setListed(boolean listed) {
        while (true) {
            State current = stateRef.get();
            if (current.listed == listed) {
                return;
            }
            State next = new State(current.leasedNamespace, current.inflightRequests,
                current.lastActivityTime, listed);
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }

    boolean tryAcquireWarmLease(String namespace, int maxConcurrency) {
        while (true) {
            State current = stateRef.get();
            if (!current.listed || !namespace.equals(current.leasedNamespace)
                || current.inflightRequests >= maxConcurrency) {
                return false;
            }
            State next = new State(current.leasedNamespace, current.inflightRequests + 1,
                System.nanoTime(), true);
            if (stateRef.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    boolean tryClaimFreshLease(String namespace) {
        while (true) {
            State current = stateRef.get();
            // Fresh pod: must have NO namespace and 0 inflight
            boolean isUnleased = current.leasedNamespace == null || current.leasedNamespace.isEmpty();
            if (!current.listed || !isUnleased || current.inflightRequests != 0) {
                return false;
            }
            State next = new State(namespace, 1, System.nanoTime(), true);
            if (stateRef.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    boolean tryStealIdleLease(String namespace) {
        while (true) {
            State current = stateRef.get();
            // Stealable pod: ANY routable pod with 0 inflight requests
            if (!current.listed || current.inflightRequests != 0) {
                return false;
            }
            State next = new State(namespace, 1, System.nanoTime(), true);
            if (stateRef.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    void decrementInflightRequests() {
        while (true) {
            State current = stateRef.get();
            State next = new State(current.leasedNamespace,
                Math.max(0, current.inflightRequests - 1), System.nanoTime(), current.listed);
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }

    /**
     * Adopts the lease ownership reported by a rejecting worker and releases the occupancy slot
     * that this proxy speculatively took for the request the worker refused.
     *
     * <p>Only the leased namespace is adopted. The worker's reported active task count is
     * deliberately not copied, because that count describes work owned by connections this proxy
     * does not hold - typically tasks dispatched by a previous proxy process that was restarted.
     * Their completion responses will never arrive on any channel this proxy owns, so a count
     * adopted from a rejection is structurally un-decrementable. Copying it would pin a phantom
     * occupancy on the pod for as long as it stays registered, permanently reducing its usable
     * concurrency and hiding it from both the fresh-claim and idle-steal selection paths.
     *
     * <p>Adopting only the namespace keeps the half of the signal that is actually useful. The pod
     * stops being a warm match for the rejected namespace, so retries are no longer pinned to the
     * worker that just rejected them and will spill to a genuinely available pod, while the
     * occupancy count continues to reflect only requests this proxy issued and will see complete.
     *
     * @param leasedNamespace the namespace the worker reports as holding the lease, or
     *     {@code null} if the worker did not report one, in which case the currently recorded
     *     owner is retained
     */
    void adoptRejectedLease(String leasedNamespace) {
        while (true) {
            State current = stateRef.get();
            String nextNamespace = leasedNamespace != null ? leasedNamespace : current.leasedNamespace;
            State next = new State(nextNamespace, Math.max(0, current.inflightRequests - 1),
                System.nanoTime(), current.listed);
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }
    
    void recordActivity() {
        while (true) {
            State current = stateRef.get();
            State next = new State(current.leasedNamespace, current.inflightRequests, System.nanoTime(),
                current.listed);
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }
}

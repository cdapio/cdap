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

/**
 * PodState represents the in-memory routing and lease status of an individual Task Worker pod.
 * Entirely lock-free, backing state via an immutable internal representation and AtomicReference CAS loops.
 */
public class PodState {
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

    public PodState(String leasedNamespace, int inflightRequests) {
        this.stateRef = new AtomicReference<>(new State(
            leasedNamespace,
            inflightRequests,
            System.nanoTime() - TimeUnit.SECONDS.toNanos(40)
        ));
    }

    public String getLeasedNamespace() {
        return stateRef.get().leasedNamespace;
    }

    public int getInflightRequests() {
        return stateRef.get().inflightRequests;
    }

    public long getLastActivityTime() {
        return stateRef.get().lastActivityTime;
    }

    public boolean tryAcquireWarmLease(String namespace, int maxConcurrency) {
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

    public boolean tryClaimIdleLease(String namespace, long idleTimeoutNanos) {
        while (true) {
            State current = stateRef.get();
            boolean isUnleased = current.leasedNamespace == null || current.leasedNamespace.isEmpty();
            boolean isExpiredIdle = current.inflightRequests == 0 
                && (System.nanoTime() - current.lastActivityTime > idleTimeoutNanos);
            
            if (current.inflightRequests != 0 || (!isUnleased && !isExpiredIdle)) {
                return false;
            }
            State next = new State(namespace, 1, System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    public void decrementInflightRequests() {
        while (true) {
            State current = stateRef.get();
            State next = new State(current.leasedNamespace, 
                Math.max(0, current.inflightRequests - 1), System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }

    public void updateFromHeader(String activeTasksStr, String leasedNamespace) {
        while (true) {
            State current = stateRef.get();
            int nextInflight = current.inflightRequests;
            if (activeTasksStr != null) {
                try {
                    nextInflight = Integer.parseInt(activeTasksStr);
                } catch (NumberFormatException e) {
                    nextInflight = Math.max(0, current.inflightRequests - 1);
                }
            } else {
                nextInflight = Math.max(0, current.inflightRequests - 1);
            }
            String nextNamespace = leasedNamespace != null ? leasedNamespace : current.leasedNamespace;
            State next = new State(nextNamespace, nextInflight, System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }
    
    public void recordActivity() {
        while (true) {
            State current = stateRef.get();
            State next = new State(current.leasedNamespace, current.inflightRequests, System.nanoTime());
            if (stateRef.compareAndSet(current, next)) {
                return;
            }
        }
    }
}

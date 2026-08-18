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

/**
 * PodState represents the in-memory routing and lease status of an individual Task Worker pod.
 *
 * <p>It tracks:
 * <ul>
 *   <li>{@code leasedNamespace}: The namespace currently pinned to this physical worker pod.
 *       Only requests belonging to this namespace may execute on this pod.</li>
 *   <li>{@code inflightRequests}: The number of active concurrent tasks running on this pod
 *       (governed up to 10 concurrent requests).</li>
 *   <li>{@code lastActivityTime}: Timestamp of the most recent request completion, used to calculate
 *       idle TTL eviction (35s) so idle pods can be reclaimed by other namespaces.</li>
 * </ul>
 */
public class PodState {
    private String leasedNamespace;
    private int inflightRequests;
    private long lastActivityTime;

    public PodState(String leasedNamespace, int inflightRequests) {
        this.leasedNamespace = leasedNamespace;
        this.inflightRequests = inflightRequests;
        // Initialize lastActivityTime with a 40s offset so a newly discovered pod is immediately eligible
        // to be claimed by any namespace upon startup.
        this.lastActivityTime = System.currentTimeMillis() - 40000L;
    }

    public String getLeasedNamespace() {
        return leasedNamespace;
    }

    public void setLeasedNamespace(String leasedNamespace) {
        this.leasedNamespace = leasedNamespace;
    }

    public int getInflightRequests() {
        return inflightRequests;
    }

    public void setInflightRequests(int inflightRequests) {
        this.inflightRequests = inflightRequests;
    }

    public long getLastActivityTime() {
        return lastActivityTime;
    }

    public void setLastActivityTime(long lastActivityTime) {
        this.lastActivityTime = lastActivityTime;
    }
}

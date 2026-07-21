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
 * Tracks the routing state and load for a given worker pod IP.
 */
public class PodState {
    private String leasedNamespace;
    private int inflightRequests;
    private long lastActivityTime;

    public PodState(String leasedNamespace, int inflightRequests) {
        this.leasedNamespace = leasedNamespace;
        this.inflightRequests = inflightRequests;
        this.lastActivityTime = 0; // Instantly trigger predictions on boot
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

/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.proto;

import com.google.gson.annotations.SerializedName;

/**
 * Metadata about a Reactor system service.
 */
public class SystemServiceMeta {

  private final String name;
  private final String description;
  private final String status;
  private final String logs;

  @SerializedName("min")
  private final int minInstances;

  @SerializedName("max")
  private final int maxInstances;

  @SerializedName("requested")
  private final int instancesRequested;

  @SerializedName("provisioned")
  private final int instancesProvisioned;


  public SystemServiceMeta(String name, String description, String status, String logs, int minInstances,
                           int maxInstances, int instancesRequested, int instancesProvisioned) {
    this.name = name;
    this.description = description;
    this.status = status;
    this.logs = logs;
    this.minInstances = minInstances;
    this.maxInstances = maxInstances;
    this.instancesRequested = instancesRequested;
    this.instancesProvisioned = instancesProvisioned;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getStatus() {
    return status;
  }

  public String getLogs() {
    return logs;
  }

  public int getMinInstances() {
    return minInstances;
  }

  public int getMaxInstances() {
    return maxInstances;
  }

  public int getInstancesRequested() {
    return instancesRequested;
  }

  public int getInstancesProvisioned() {
    return instancesProvisioned;
  }
}

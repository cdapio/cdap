/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.proto;

import java.util.Set;

/**
 * Class to describe status of restart instances of service request.
 */
public class RestartServiceInstancesStatus {
  private final Set<Integer> instanceIds;
  private final String serviceName;
  private final long startTimeInMs;
  private final long endTimeInMs;
  private final RestartStatus status;

  public RestartServiceInstancesStatus(String serviceName, long startMs, long endMs, RestartStatus status,
                                       Set<Integer> instanceIds) {
    this.serviceName = serviceName;
    this.startTimeInMs = startMs;
    this.endTimeInMs = endMs;
    this.status = status;
    this.instanceIds = instanceIds;
  }

  public Set<Integer> getInstanceIds() {
    return instanceIds;
  }

  public String getServiceName() {
    return serviceName;
  }

  public long getStartTimeInMs() {
    return startTimeInMs;
  }

  public long getEndTimeInMs() {
    return endTimeInMs;
  }

  public RestartStatus getStatus() {
    return status;
  }

  /**
   * Defining the status of the restart instances request.
   */
  public enum RestartStatus {
    SUCCESS, FAILURE;
  }
}

/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.status;

import java.util.List;
import java.util.ArrayList;

/**
 * Status when generating Support bundle.
 */
public class SupportBundleStatus {
  /** UUID of the bundle status object describes */
  private final String bundleId;
  /** status of bundle collection (IN_PROGRESS/FINISHED/FAILED) */
  private final CollectionState status;
  /** Failed bundle describes the failure */
  private String statusDetails;
  /** when bundle collection was started */
  private final long startTimestamp;
  /** FINISHED/FAILED bundles when bundle collection was completed */
  private long finishTimestamp;
  // any parameters passed to start collection
  private final SupportBundleConfiguration parameters;
  // Array of top-level tasks for the bundle, see task structure below
  private List<SupportBundleTaskStatus> tasks = new ArrayList<>();

  public SupportBundleStatus(String bundleId, long startTimestamp, SupportBundleConfiguration parameters,
                             CollectionState status) {
    this.bundleId = bundleId;
    this.startTimestamp = startTimestamp;
    this.parameters = parameters;
    this.status = status;
  }

  public SupportBundleStatus(SupportBundleStatus supportBundleStatus, String statusDetails, CollectionState status,
                             long finishTimestamp) {
    this.bundleId = supportBundleStatus.getBundleId();
    this.startTimestamp = supportBundleStatus.getStartTimestamp();
    this.parameters = supportBundleStatus.getParameters();
    this.statusDetails = statusDetails;
    this.finishTimestamp = finishTimestamp;
    this.status = status;
  }

  /**
   * Get support bundle generation status
   */
  public CollectionState getStatus() {
    return status;
  }

  /**
   * Get support bundle generation status details
   */
  public String getStatusDetails() {
    return statusDetails;
  }

  /**
   * Get support bundle generation start time
   */
  public Long getStartTimestamp() {
    return startTimestamp;
  }

  /**
   * Get support bundle generation finish time
   */
  public long getFinishTimestamp() {
    return finishTimestamp;
  }

  /**
   * Get support bundle generation request parameters
   */
  public SupportBundleConfiguration getParameters() {
    return parameters;
  }

  /**
<<<<<<< HEAD
   * Get support bundle generation id
=======
   * Set support bundle generation id
>>>>>>> 2ab03a9aa7a (revised the comments)
   */
  public String getBundleId() {
    return bundleId;
  }

  /**
   * Get support bundle generation tasks
   */
  public List<SupportBundleTaskStatus> getTasks() {
    return tasks;
  }
}

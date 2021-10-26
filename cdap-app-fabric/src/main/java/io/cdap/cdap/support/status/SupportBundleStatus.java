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

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/**
 * Status when generating Support bundle
 */
public class SupportBundleStatus {
  // UUID of the bundle status object describes
  private String bundleId;
  // status of bundle collection (IN_PROGRESS/FINISHED/FAILED)
  private CollectionState status;
  // Failed bundle describes the failure
  private String statusDetails;
  // when bundle collection was started
  private Long startTimestamp;
  // FINISHED/FAILED bundles when bundle collection was completed
  private Long finishTimestamp;
  // any parameters passed to start collection
  private SupportBundleConfiguration parameters;
  // Array of top-level tasks for the bundle, see task structure below
  private List<SupportBundleTaskStatus> tasks = new ArrayList<>();

  /** Get support bundle generation status */
  public CollectionState getStatus() {
    return status;
  }

  /** Set support bundle generation status */
  public void setStatus(CollectionState status) {
    this.status = status;
  }

  /** Get support bundle generation status details */
  public String getStatusDetails() {
    return statusDetails;
  }

  /** Set support bundle generation status details */
  public void setStatusDetails(String statusDetails) {
    this.statusDetails = statusDetails;
  }

  /** Get support bundle generation start time */
  public Long getStartTimestamp() {
    return startTimestamp;
  }

  /** Set support bundle generation start time */
  public void setStartTimestamp(Long startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  /** Get support bundle generation finish time */
  public Long getFinishTimestamp() {
    return finishTimestamp;
  }

  /** Set support bundle generation finish time */
  public void setFinishTimestamp(Long finishTimestamp) {
    this.finishTimestamp = finishTimestamp;
  }

  /** Get support bundle generation request parameters */
  public SupportBundleConfiguration getParameters() {
    return parameters;
  }

  /** Set support bundle generation request parameters */
  public void setParameters(SupportBundleConfiguration parameters) {
    this.parameters = parameters;
  }

  /** Get support bundle generation tasks */
  public List<SupportBundleTaskStatus> getTasks() {
    return tasks;
  }

  /** Set support bundle generation tasks */
  public void setTasks(List<SupportBundleTaskStatus> tasks) {
    this.tasks = tasks;
  }

  /** Set support bundle generation id */
  public String getBundleId() {
    return bundleId;
  }

  /** Set support bundle generation id */
  public void setBundleId(String bundleId) {
    this.bundleId = bundleId;
  }
}

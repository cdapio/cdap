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

import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.util.Collections;
import java.util.Set;

/**
 * Status when generating Support bundle.
 */
public class SupportBundleStatus {
  /**
   * UUID of the bundle status object describes
   */
  private final String bundleId;
  /**
   * status of bundle collection (IN_PROGRESS/FINISHED/FAILED)
   */
  private final CollectionState status;
  /**
   * Failed bundle describes the failure
   */
  private final String statusDetails;
  /**
   * when bundle collection was started
   */
  private final Long startTimestamp;
  /**
   * FINISHED/FAILED bundles when bundle collection was completed
   */
  private final Long finishTimestamp;
  // any parameters passed to start collection
  private final SupportBundleConfiguration parameters;
  // Array of top-level tasks for the bundle, see task structure below
  private final Set<SupportBundleTaskStatus> tasks;

  private SupportBundleStatus(String bundleId, Long startTimestamp, SupportBundleConfiguration parameters,
                              String statusDetails, CollectionState status, Long finishTimestamp,
                              Set<SupportBundleTaskStatus> tasks) {
    this.bundleId = bundleId;
    this.startTimestamp = startTimestamp;
    this.parameters = parameters;
    this.statusDetails = statusDetails;
    this.finishTimestamp = finishTimestamp;
    this.status = status;
    this.tasks = tasks;
  }

  /**
   * @return Builder to create a SupportBundleStatus
   */
  public static SupportBundleStatus.Builder builder() {
    return new SupportBundleStatus.Builder();
  }

  /**
   * @param previousStatus outdated status
   * @return Builder to create a SupportBundleStatus, initialized with values from the specified existing status
   */
  public static SupportBundleStatus.Builder builder(SupportBundleStatus previousStatus) {
    return new SupportBundleStatus.Builder(previousStatus);
  }

  /**
   * Builder to build bundle task status.
   */
  public static class Builder {
    private String bundleId;
    private CollectionState status;
    private String statusDetails;
    private Long startTimestamp;
    private SupportBundleConfiguration parameters;
    private Set<SupportBundleTaskStatus> tasks;
    private Long finishTimestamp;

    private Builder() {
      this.tasks = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    private Builder(SupportBundleStatus previousStatus) {
      this.bundleId = previousStatus.getBundleId();
      this.startTimestamp = previousStatus.getStartTimestamp();
      this.parameters = previousStatus.getParameters();
      this.tasks = previousStatus.getTasks();
    }

    /**
     * Set support bundle bundle id
     */
    public SupportBundleStatus.Builder setBundleId(String bundleId) {
      this.bundleId = bundleId;
      return this;
    }

    /**
     * Set support bundle status detail
     */
    public SupportBundleStatus.Builder setStatusDetails(String statusDetails) {
      this.statusDetails = statusDetails;
      return this;
    }

    /**
     * Set support bundle start time
     */
    public SupportBundleStatus.Builder setStartTimestamp(long startTimestamp) {
      this.startTimestamp = startTimestamp;
      return this;
    }

    /**
     * Set support bundle generation subtask status
     */
    public SupportBundleStatus.Builder setParameters(SupportBundleConfiguration parameters) {
      this.parameters = parameters;
      return this;
    }

    /**
     * Set support bundle tasks
     */
    public SupportBundleStatus.Builder setTasks(Set<SupportBundleTaskStatus> tasks) {
      this.tasks = tasks;
      return this;
    }

    /**
     * Set support bundle finish time
     */
    public SupportBundleStatus.Builder setFinishTimestamp(long finishTimestamp) {
      this.finishTimestamp = finishTimestamp;
      return this;
    }

    /**
     * Set support bundle status
     */
    public SupportBundleStatus.Builder setStatus(CollectionState status) {
      this.status = status;
      return this;
    }

    /**
     * Initialize the bundle with new status and add finish time stamp
     */
    public SupportBundleStatus build() {
      if (bundleId == null) {
        throw new IllegalArgumentException("Bundle id must be specified.");
      }
      if (status == null) {
        throw new IllegalArgumentException("Bundle status must be specified.");
      }
      return new SupportBundleStatus(bundleId, startTimestamp, parameters, statusDetails, status, finishTimestamp,
                                     tasks);
    }
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
   * Get support bundle generation id
   */
  public String getBundleId() {
    return bundleId;
  }

  /**
   * Get support bundle generation tasks
   */
  public Set<SupportBundleTaskStatus> getTasks() {
    return tasks;
  }
}

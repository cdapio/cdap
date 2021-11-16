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

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/**
 * Each individual task status when generating support bundle.
 */
public class SupportBundleTaskStatus {
  // unique task name, also defines directory task stores resulting files in
  private final String name;
  // task class name
  private final String type;
  // status for task (QUEUED/IN_PROGRESS/FINISHED/FAILED)
  private final CollectionState status;
  // if task was retried, number of retries, otherwise 0
  private final int retries;
  // array of subtasks (if any)
  @SerializedName("sub-tasks")
  private final List<SupportBundleTaskStatus> subTasks;
  // if task was already started, timestamp of the last start
  @SerializedName("start-timestamp")
  private final Long startTimestamp;
  // if the task was already finished, timestamp of the last finish Cleared when the task started
  // after retry and repopulated on subsequent finish
  @SerializedName("finish-timestamp")
  private final Long finishTimestamp;

  private SupportBundleTaskStatus(String name, String type, Long startTimestamp, List<SupportBundleTaskStatus> subTasks,
                                 int retries, Long finishTimestamp, CollectionState status) {
    this.name = name;
    this.type = type;
    this.startTimestamp = startTimestamp;
    this.subTasks = ImmutableList.copyOf(subTasks);
    this.retries = retries;
    this.finishTimestamp = finishTimestamp;
    this.status = status;
  }

  /**
   * @return Builder to create a SupportBundleTaskStatus
   */
  public static SupportBundleTaskStatus.Builder builder() {
    return new SupportBundleTaskStatus.Builder();
  }

  /**
   * @param previousTaskStatus outdated task status
   * @return Builder to create a SupportBundleTaskStatus, initialized with values from the specified existing status
   */
  public static SupportBundleTaskStatus.Builder builder(SupportBundleTaskStatus previousTaskStatus) {
    return new SupportBundleTaskStatus.Builder(previousTaskStatus);
  }

  /**
   * Builder to build bundle task status.
   */
  public static class Builder {
    private String name;
    private String type;
    private Long startTimestamp;
    private List<SupportBundleTaskStatus> subTasks;
    private Integer retries;
    private Long finishTimestamp;
    private CollectionState status;

    private Builder() {
      this.retries = 0;
      this.subTasks = new ArrayList<>();
    }

    private Builder(SupportBundleTaskStatus previousTaskStatus) {
      this.name = previousTaskStatus.getName();
      this.type = previousTaskStatus.getType();
      this.startTimestamp = previousTaskStatus.getStartTimestamp();
      this.subTasks = previousTaskStatus.getSubTasks();
      this.retries = previousTaskStatus.getRetries();
    }

    /**
     * Set support bundle generation name
     */
    public SupportBundleTaskStatus.Builder setName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set support bundle generation task type
     */
    public SupportBundleTaskStatus.Builder setType(String type) {
      this.type = type;
      return this;
    }

    /**
     * Set support bundle generation task start time
     */
    public SupportBundleTaskStatus.Builder setStartTimestamp(long startTimestamp) {
      this.startTimestamp = startTimestamp;
      return this;
    }

    /**
     * Set support bundle generation subtask status
     */
    public SupportBundleTaskStatus.Builder setSubTasks(List<SupportBundleTaskStatus> subTasks) {
      this.subTasks = subTasks;
      return this;
    }

    /**
     * Set support bundle generation task retry times
     */
    public SupportBundleTaskStatus.Builder setRetries(Integer retries) {
      this.retries = retries;
      return this;
    }

    /**
     * Set support bundle generation task finish time
     */
    public SupportBundleTaskStatus.Builder setFinishTimestamp(long finishTimestamp) {
      this.finishTimestamp = finishTimestamp;
      return this;
    }

    /**
     * Set support bundle generation task status
     */
    public SupportBundleTaskStatus.Builder setStatus(CollectionState status) {
      this.status = status;
      return this;
    }

    /**
     * Initialize the bundle task status
     */
    public SupportBundleTaskStatus build() {
      if (name == null) {
        throw new IllegalArgumentException("Bundle task name must be specified.");
      }
      if (type == null) {
        throw new IllegalArgumentException("Bundle task type must be specified.");
      }
      if (status == null) {
        throw new IllegalArgumentException("Bundle task status must be specified.");
      }
      return new SupportBundleTaskStatus(name, type, startTimestamp, subTasks, retries, finishTimestamp, status);
    }
  }

  /**
   * Get support bundle generation task name
   */
  public String getName() {
    return name;
  }

  /**
   * Get support bundle generation task type
   */
  public String getType() {
    return type;
  }

  /**
   * Get support bundle generation task status
   */
  public CollectionState getStatus() {
    return status;
  }

  /**
   * Get support bundle generation task retry times
   */
  public int getRetries() {
    return retries;
  }

  /**
   * Get support bundle generation subtask status
   */
  public List<SupportBundleTaskStatus> getSubTasks() {
    return subTasks;
  }

  /**
   * Get support bundle generation task start time
   */
  public long getStartTimestamp() {
    return startTimestamp;
  }

  /**
   * Get support bundle generation task finish time
   */
  public long getFinishTimestamp() {
    return finishTimestamp;
  }
}

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

public class SupportBundleStatusTask {
  // unique task name, also defines directory task stores resulting files in
  @SerializedName("name")
  private String name;
  // task class name
  @SerializedName("type")
  private String type;
  // status for task (QUEUED/IN_PROGRESS/FINISHED/FAILED)
  @SerializedName("status")
  private String status;
  // if task was retried, number of retries, otherwise 0
  @SerializedName("retries")
  private int retries = 0;
  // array of subtasks (if any)
  @SerializedName("sub-tasks")
  private List<SupportBundleStatusTask> subTasks = new ArrayList<>();
  // if task was already started, timestamp of the last start
  @SerializedName("start-timestamp")
  private Long startTimestamp;
  // if the task was already finished, timestamp of the last finish Cleared when the task started
  // after retry and repopulated on subsequent finish
  @SerializedName("finish-timestamp")
  private Long finishTimestamp;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public int getRetries() {
    return retries;
  }

  public void setRetries(int retries) {
    this.retries = retries;
  }

  public List<SupportBundleStatusTask> getSubTasks() {
    return subTasks;
  }

  public void setSubTasks(List<SupportBundleStatusTask> subTasks) {
    this.subTasks = subTasks;
  }

  public Long getStartTimestamp() {
    return startTimestamp;
  }

  public void setStartTimestamp(Long startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  public Long getFinishTimestamp() {
    return finishTimestamp;
  }

  public void setFinishTimestamp(Long finishTimestamp) {
    this.finishTimestamp = finishTimestamp;
  }
}

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

public class SupportBundleCreationQueryParameters {
  @SerializedName("namespace-id")
  private String namespaceId;

  @SerializedName("app-id")
  private String appId;

  @SerializedName("workflow-name")
  private String workflowName;

  @SerializedName("run-id")
  private String runId;
  // num of run log customer request for each pipeline run
  @SerializedName("num-run-log")
  private Integer numOfRunLog;

  public String getNamespaceId() {
    return namespaceId;
  }

  public void setNamespaceId(String namespaceId) {
    this.namespaceId = namespaceId;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getWorkflowName() {
    return workflowName;
  }

  public void setWorkflowName(String workflowName) {
    this.workflowName = workflowName;
  }

  public String getRunId() {
    return runId;
  }

  public void setRunId(String runId) {
    this.runId = runId;
  }

  public Integer getNumOfRunLog() {
    return numOfRunLog;
  }

  public void setNumOfRunLog(Integer numOfRunLog) {
    this.numOfRunLog = numOfRunLog;
  }
}

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

import javax.annotation.Nullable;

/**
 * Support bundle configuration for gathering post api parameters
 */
public class SupportBundleConfiguration {
  /** pipeline namespace id */
  private final String namespaceId;
  /** pipeline application id */
  private final String appId;
  /** pipeline workflow name */
  private final String workflowName;
  /** pipeline run id */
  private final String runId;
  // num of run log customer request for each pipeline run
  private final int numOfRunLog;

  public SupportBundleConfiguration(@Nullable String namespaceId,
                                    @Nullable String appId,
                                    @Nullable String runId,
                                    @Nullable String workflowName,
                                    int numOfRunLog) {
    this.namespaceId = namespaceId;
    this.appId = appId;
    this.runId = runId;
    this.workflowName = workflowName;
    this.numOfRunLog = numOfRunLog;
  }

  public String getNamespaceId() {
    return namespaceId;
  }

  public String getAppId() {
    return appId;
  }

  public String getWorkflowName() {
    return workflowName;
  }

  public String getRunId() {
    return runId;
  }

  public Integer getNumOfRunLog() {
    return numOfRunLog;
  }
}

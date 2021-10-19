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

package io.cdap.cdap.support;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.status.SupportBundleConfiguration;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Support bundle state for handling all assisted parameters inside the task factories
 */
public class SupportBundleState {
  /** unique support bundle id */
  private String uuid;
  /** pipeline namespace id */
  private String namespaceId;
  /** pipeline application id */
  private String appId;
  /** pipeline run id */
  private String runId;
  /** support bundle base path */
  private String basePath;
  /** support bundle system log path */
  private String systemLogPath;
  /** parameter request for num of run log needed from customer */
  private Integer numOfRunLogNeeded;
  /** pipeline workflow name */
  private String workflowName;
  /** all the namespace under the pipeline */
  private List<String> namespaceList;
  /** support bundle job to process all the tasks */
  private SupportBundleJob supportBundleJob;
  /** support bundle max runs fetch for each namespace */
  private Integer maxRunsPerNamespace;

  public SupportBundleState(SupportBundleConfiguration supportBundleConfiguration) {
    this.appId = supportBundleConfiguration.getAppId();
    this.runId = supportBundleConfiguration.getRunId();
    this.workflowName = supportBundleConfiguration.getWorkflowName();
    this.numOfRunLogNeeded = supportBundleConfiguration.getNumOfRunLog();
  }

  /** Get support bundle id */
  public String getUuid() {
    return uuid;
  }
  /** Set support bundle id */
  public void setUuid(String uuid) {
    this.uuid = uuid;
  }
  /** Get pipeline namespace id */
  public String getNamespaceId() {
    return namespaceId;
  }
  /** Set pipeline namespace id */
  public void setNamespaceId(@Nullable String namespaceId) {
    this.namespaceId = namespaceId;
  }
  /** Get pipeline Application id */
  public String getAppId() {
    return appId;
  }
  /** Set pipeline Application id */
  public void setAppId(@Nullable String appId) {
    this.appId = appId;
  }
  /** Get pipeline run id */
  public String getRunId() {
    return runId;
  }
  /** Set pipeline run id */
  public void setRunId(@Nullable String runId) {
    this.runId = runId;
  }
  /** Get support bundle base path */
  public String getBasePath() {
    return basePath;
  }
  /** Set support bundle base path */
  public void setBasePath(@Nullable String basePath) {
    this.basePath = basePath;
  }
  /** Get support bundle system log path */
  public String getSystemLogPath() {
    return systemLogPath;
  }
  /** Set support bundle system log path */
  public void setSystemLogPath(@Nullable String systemLogPath) {
    this.systemLogPath = systemLogPath;
  }
  /** Get number of run log needed for each pipeline run */
  public int getNumOfRunLogNeeded() {
    return numOfRunLogNeeded;
  }
  /** Set number of run log needed for each pipeline run */
  public void setNumOfRunLogNeeded(int numOfRunLogNeeded) {
    this.numOfRunLogNeeded = numOfRunLogNeeded;
  }
  /** Get workflow name */
  public String getWorkflowName() {
    return workflowName;
  }
  /** Set workflow name */
  public void setWorkflowName(@Nullable String workflowName) {
    this.workflowName = workflowName;
  }
  /** Get list of namespace */
  public List<String> getNamespaceList() {
    return namespaceList;
  }
  /** Set list of namespace */
  public void setNamespaceList(List<String> namespaceList) {
    this.namespaceList = ImmutableList.copyOf(namespaceList);
  }
  /** Get support bundle job */
  public SupportBundleJob getSupportBundleJob() {
    return supportBundleJob;
  }
  /** Set support bundle job */
  public void setSupportBundleJob(SupportBundleJob supportBundleJob) {
    this.supportBundleJob = supportBundleJob;
  }
  /** Get max runs allowed for each namespace */
  public Integer getMaxRunsPerNamespace() {
    return maxRunsPerNamespace;
  }
  /** Set max runs allowed for each namespace */
  public void setMaxRunsPerNamespace(Integer maxRunsPerNamespace) {
    this.maxRunsPerNamespace = maxRunsPerNamespace;
  }
}

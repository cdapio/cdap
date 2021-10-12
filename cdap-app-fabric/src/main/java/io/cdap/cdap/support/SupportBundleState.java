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
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.status.SupportBundleConfiguration;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Support bundle state for handling all assisted parameters inside the task factories
 */
public class SupportBundleState {
  private String namespaceId;
  private String appId;
  private String runId;
  private String basePath;

  private String systemLogPath;
  private Integer numOfRunLogNeeded;
  private String workflowName;
  private List<ApplicationRecord> applicationRecordList;
  private List<String> namespaceList;
  private SupportBundleJob supportBundleJob;

  public SupportBundleState(SupportBundleConfiguration supportBundleConfiguration) {
    this.appId = supportBundleConfiguration.getAppId();
    this.runId = supportBundleConfiguration.getRunId();
    this.workflowName = supportBundleConfiguration.getWorkflowName();
    this.numOfRunLogNeeded = supportBundleConfiguration.getNumOfRunLog();
  }

  public String getNamespaceId() {
    return namespaceId;
  }

  public void setNamespaceId(@Nullable String namespaceId) {
    this.namespaceId = namespaceId;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(@Nullable String appId) {
    this.appId = appId;
  }

  public String getRunId() {
    return runId;
  }

  public void setRunId(@Nullable String runId) {
    this.runId = runId;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(@Nullable String basePath) {
    this.basePath = basePath;
  }

  public String getSystemLogPath() {
    return systemLogPath;
  }

  public void setSystemLogPath(@Nullable String systemLogPath) {
    this.systemLogPath = systemLogPath;
  }

  public int getNumOfRunLogNeeded() {
    return numOfRunLogNeeded;
  }

  public void setNumOfRunLogNeeded(int numOfRunLogNeeded) {
    this.numOfRunLogNeeded = numOfRunLogNeeded;
  }

  public String getWorkflowName() {
    return workflowName;
  }

  public void setWorkflowName(@Nullable String workflowName) {
    this.workflowName = workflowName;
  }

  public List<ApplicationRecord> getApplicationRecordList() {
    return applicationRecordList;
  }

  public void setApplicationRecordList(List<ApplicationRecord> applicationRecordList) {
    this.applicationRecordList = ImmutableList.copyOf(applicationRecordList);
  }

  public List<String> getNamespaceList() {
    return namespaceList;
  }

  public void setNamespaceList(List<String> namespaceList) {
    this.namespaceList = ImmutableList.copyOf(namespaceList);
  }

  public SupportBundleJob getSupportBundleJob() {
    return supportBundleJob;
  }

  public void setSupportBundleJob(SupportBundleJob supportBundleJob) {
    this.supportBundleJob = supportBundleJob;
  }
}

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

package io.cdap.cdap.support.conf;

import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.MetricsClient;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.support.status.SupportBundleStatus;

import java.util.List;

public class SupportBundleConfiguration {
  private SupportBundleStatus supportBundleStatus;
  private String namespaceId;
  private String appId;
  private String runId;
  private String basePath;

  private String systemLogPath;
  private int numOfRunLogNeeded;
  private String workflowName;
  private List<ApplicationRecord> applicationRecordList;
  private List<String> namespaceList;
  private ProgramClient programClient;
  private NamespaceClient namespaceClient;
  private ApplicationClient applicationClient;
  private MetricsClient metricsClient;

  public SupportBundleStatus getSupportBundleStatus() {
    return supportBundleStatus;
  }

  public void setSupportBundleStatus(SupportBundleStatus supportBundleStatus) {
    this.supportBundleStatus = supportBundleStatus;
  }

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

  public String getRunId() {
    return runId;
  }

  public void setRunId(String runId) {
    this.runId = runId;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public String getSystemLogPath() {
    return systemLogPath;
  }

  public void setSystemLogPath(String systemLogPath) {
    this.systemLogPath = systemLogPath;
  }

  public int getNumOfRunLogNeeded() {
    return numOfRunLogNeeded;
  }

  public void setNumOfRunLogNeeded(Integer numOfRunLogNeeded) {
    this.numOfRunLogNeeded = numOfRunLogNeeded == null ? 1 : numOfRunLogNeeded;
  }

  public String getWorkflowName() {
    return workflowName;
  }

  public void setWorkflowName(String workflowName) {
    this.workflowName =
        workflowName == null || workflowName.length() == 0 ? "DataPipelineWorkflow" : workflowName;
  }

  public List<ApplicationRecord> getApplicationRecordList() {
    return applicationRecordList;
  }

  public void setApplicationRecordList(List<ApplicationRecord> applicationRecordList) {
    this.applicationRecordList = applicationRecordList;
  }

  public List<String> getNamespaceList() {
    return namespaceList;
  }

  public void setNamespaceList(List<String> namespaceList) {
    this.namespaceList = namespaceList;
  }

  public ProgramClient getProgramClient() {
    return programClient;
  }

  public void setProgramClient(ProgramClient programClient) {
    this.programClient = programClient;
  }

  public NamespaceClient getNamespaceClient() {
    return namespaceClient;
  }

  public void setNamespaceClient(NamespaceClient namespaceClient) {
    this.namespaceClient = namespaceClient;
  }

  public ApplicationClient getApplicationClient() {
    return applicationClient;
  }

  public void setApplicationClient(ApplicationClient applicationClient) {
    this.applicationClient = applicationClient;
  }

  public MetricsClient getMetricsClient() {
    return metricsClient;
  }

  public void setMetricsClient(MetricsClient metricsClient) {
    this.metricsClient = metricsClient;
  }
}

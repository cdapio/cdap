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
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.status.SupportBundleConfiguration;

import java.util.List;

/**
 * Support bundle task configuration for handling all assisted parameters inside the task factories
 * tasks will each fetch pipeline and run info and generate files inside the support bundle in parallel
 */
public class SupportBundleTaskConfiguration {
  /**
   * unique support bundle id
   */
  private String uuid;
  /**
   * pipeline application id
   */
  private String appId;
  /**
   * pipeline run id
   */
  private String runId;
  /**
   * support bundle base path
   */
  private String basePath;
  /**
   * pipeline program name
   */
  private String programName;
  /**
   * all the namespace under the pipeline
   */
  private List<NamespaceId> namespaces;
  /**
   * support bundle job to process all the tasks
   */
  private SupportBundleJob supportBundleJob;
  /**
   * support bundle max runs fetch for each pipeline
   */
  private Integer maxRunsPerPipeline;

  public SupportBundleTaskConfiguration(SupportBundleConfiguration supportBundleConfiguration, String uuid,
                                        String basePath, List<NamespaceId> namespaces,
                                        SupportBundleJob supportBundleJob) {
    this.appId = supportBundleConfiguration.getApp();
    this.runId = supportBundleConfiguration.getRun();
    this.programName = supportBundleConfiguration.getProgramName();
    this.uuid = uuid;
    this.basePath = basePath;
    this.namespaces = ImmutableList.copyOf(namespaces);
    this.supportBundleJob = supportBundleJob;
  }

  /**
   * Get support bundle id
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * Get pipeline Application id
   */
  public String getAppId() {
    return appId;
  }

  /**
   * Get pipeline run id
   */
  public String getRunId() {
    return runId;
  }

  /**
   * Get support bundle base path
   */
  public String getBasePath() {
    return basePath;
  }

  /**
   * Get workflow name
   */
  public String getProgramName() {
    return programName;
  }

  /**
   * Get list of namespace
   */
  public List<NamespaceId> getNamespaces() {
    return namespaces;
  }

  /**
   * Get support bundle job
   */
  public SupportBundleJob getSupportBundleJob() {
    return supportBundleJob;
  }

  /**
   * Get max runs allowed for each pipeline
   */
  public Integer getMaxRunsPerPipeline() {
    return maxRunsPerPipeline;
  }
}

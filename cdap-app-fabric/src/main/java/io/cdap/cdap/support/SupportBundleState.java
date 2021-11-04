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

/**
 * Support bundle state for handling all assisted parameters inside the task factories.
 */
public class SupportBundleState {
  /**
   * unique support bundle id
   */
  private final String uuid;
  /**
   * pipeline application id
   */
  private final String appId;
  /**
   * pipeline run id
   */
  private final String runId;
  /**
   * support bundle base path
   */
  private final String basePath;
  /**
   * pipeline program type
   */
  private final String programType;
  /**
   * pipeline program name
   */
  private final String programName;
  /**
   * all the namespace under the pipeline
   */
  private final List<String> namespaceList;
  /**
   * support bundle job to process all the tasks
   */
  private final SupportBundleJob supportBundleJob;
  /**
   * support bundle max run per program
   */
  private final Integer maxRunsPerProgram;

  public SupportBundleState(SupportBundleConfiguration supportBundleConfiguration, String uuid, String basePath,
                            List<String> namespaceList, SupportBundleJob supportBundleJob) {
    this.appId = supportBundleConfiguration.getAppId();
    this.runId = supportBundleConfiguration.getRunId();
    this.programType = supportBundleConfiguration.getProgramType();
    this.programName = supportBundleConfiguration.getProgramName();
    this.maxRunsPerProgram = supportBundleConfiguration.getMaxRunsPerProgram();
    this.uuid = uuid;
    this.basePath = basePath;
    this.namespaceList = ImmutableList.copyOf(namespaceList);
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
   * Get support bundle program type
   */
  public String getProgramType() {
    return programType;
  }

  /**
   * Get support bundle program name
   */
  public String getProgramName() {
    return programName;
  }

  /**
   * Get list of namespace
   */
  public List<String> getNamespaceList() {
    return namespaceList;
  }

  /**
   * Get support bundle job
   */
  public SupportBundleJob getSupportBundleJob() {
    return supportBundleJob;
  }

  /**
   * Get support bundle max run per program
   */
  public int getMaxRunsPerProgram() {
    return maxRunsPerProgram;
  }
}

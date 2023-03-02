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

import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Support bundle state for handling all assisted parameters inside the task factories.
 */
public class SupportBundleTaskConfiguration {

  /**
   * unique support bundle id
   */
  private final String uuid;
  /**
   * pipeline application id
   */
  private final String app;
  /**
   * pipeline run id
   */
  private final String run;
  /**
   * support bundle base path
   */
  private final File basePath;
  /**
   * pipeline program name
   */
  private final String programName;
  /**
   * pipeline program type
   */
  private final ProgramType programType;
  /**
   * all the namespace under the pipeline
   */
  private final List<NamespaceId> namespaces;
  /**
   * support bundle job to process all the tasks
   */
  private final SupportBundleJob supportBundleJob;
  /**
   * support bundle max run per program
   */
  private final Integer maxRunsPerProgram;

  public SupportBundleTaskConfiguration(SupportBundleConfiguration supportBundleConfiguration,
      String uuid,
      File basePath, List<NamespaceId> namespaces,
      SupportBundleJob supportBundleJob) {
    this.app = supportBundleConfiguration.getApp();
    this.run = supportBundleConfiguration.getRun();
    this.programType = supportBundleConfiguration.getProgramType();
    this.programName = supportBundleConfiguration.getProgramName();
    this.maxRunsPerProgram = supportBundleConfiguration.getMaxRunsPerProgram();
    this.uuid = uuid;
    this.basePath = basePath;
    this.namespaces = Collections.unmodifiableList(new ArrayList<>(namespaces));
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
  public String getApp() {
    return app;
  }

  /**
   * Get pipeline run id
   */
  public String getRun() {
    return run;
  }

  /**
   * Get support bundle base path
   */
  public File getBasePath() {
    return basePath;
  }

  /**
   * Get support bundle program type
   */
  public ProgramType getProgramType() {
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
   * Get support bundle max run per program
   */
  public int getMaxRunsPerProgram() {
    return maxRunsPerProgram;
  }
}

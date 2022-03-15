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

import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;

import javax.annotation.Nullable;

/**
 * Support bundle configuration for gathering post api parameters.
 */
public class SupportBundleConfiguration {
  /**
   * pipeline namespace id
   */
  @Nullable
  private final String namespace;
  /**
   * pipeline application id
   */
  @Nullable
  private final String app;
  /**
   * pipeline program type
   */
  private final String programType;
  /**
   * pipeline program name
   */
  private final String programName;
  /**
   * pipeline run id
   */
  @Nullable
  private final String run;
  /** max num of run log customer request for each program run */
  private final int maxRunsPerProgram;

  public SupportBundleConfiguration(@Nullable String namespace, @Nullable String app,
                                    @Nullable String run, String programType, String programName,
                                    int maxRunsPerProgram) {
    this.namespace = namespace;
    this.app = app;
    this.run = run;
    this.programType = programType;
    this.programName = programName;
    this.maxRunsPerProgram = maxRunsPerProgram;
  }

  /**
   * Get pipeline namespace id
   */
  @Nullable
  public String getNamespace() {
    return namespace;
  }

  /**
   * Get pipeline application id
   */
  @Nullable
  public String getApp() {
    return app;
  }

  /**
   * Get instance program type
   */
  public String getProgramType() {
    return programType;
  }

  /**
   * Get instance program name
   */
  public String getProgramName() {
    return programName;
  }

  /**
   * Get pipeline run id
   */
  @Nullable
  public String getRun() {
    return run;
  }

  /**
   * Get num of run log needed for each run
   */
  public Integer getMaxRunsPerProgram() {
    return maxRunsPerProgram;
  }
}

/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.report.main;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Fields provided at program start
 */
public class ProgramStartInfo {
  private Map<String, String> runtimeArguments;
  private ArtifactId artifactId;
  private String principal;
  private Map<String, String> systemArguments;

  public ProgramStartInfo(Map<String, String> arguments, ArtifactId artifactId, String principal,
                          Map<String, String> systemArguments) {
    this.runtimeArguments = arguments;
    this.artifactId = artifactId;
    this.principal = principal;
    this.systemArguments = systemArguments;
  }

  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  public ArtifactId getArtifactId() {
    return artifactId;
  }

  /**
   * Get the system arguemnts map
   */
  public Map<String, String> getSystemArguments() {
    return systemArguments;
  }

  /**
   * null when kerberos is not enabled in the cluster
   * @return
   */
  @Nullable
  public String getPrincipal() {
    return principal;
  }
}

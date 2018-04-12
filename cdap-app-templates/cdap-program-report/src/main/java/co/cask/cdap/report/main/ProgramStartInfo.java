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

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Fields provided at program start
 */
public class ProgramStartInfo {
  private Map<String, String> runtimeArguments;
  private ArtifactId artifactId;
  private String principal;

  ProgramStartInfo(Map<String, String> arguments, ArtifactId artifactId, String principal) {
    this.runtimeArguments = arguments;
    this.artifactId = artifactId;
    this.principal = principal;
  }

  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  public ArtifactId getArtifactId() {
    return artifactId;
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

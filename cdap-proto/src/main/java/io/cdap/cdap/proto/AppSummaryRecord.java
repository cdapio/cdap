/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import io.cdap.cdap.api.artifact.ArtifactSummary;
import javax.annotation.Nullable;

/**
 * A summary record of an application, containing its metadata, run count, and latest run.
 */
public class AppSummaryRecord {

  private final String name;
  private final String version;
  private final String description;
  private final ArtifactSummary artifact;
  private final long totalRunCount;
  @Nullable
  private final RunRecord latestRun;

  /**
   * Constructs an instance of {@link AppSummaryRecord}.
   *
   * @param name          the application name
   * @param version       the application version
   * @param description   the application description
   * @param artifact      the artifact summary
   * @param totalRunCount total number of runs for this application
   * @param latestRun     the latest run record, or null if none
   */
  public AppSummaryRecord(String name, String version, String description,
      ArtifactSummary artifact, long totalRunCount,
      @Nullable RunRecord latestRun) {
    this.name = name;
    this.version = version;
    this.description = description;
    this.artifact = artifact;
    this.totalRunCount = totalRunCount;
    this.latestRun = latestRun;
  }

  /**
   * Returns the application name.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the application version.
   */
  public String getVersion() {
    return version;
  }

  /**
   * Returns the application description.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Returns the artifact summary.
   */
  public ArtifactSummary getArtifact() {
    return artifact;
  }

  /**
   * Returns the total run count.
   */
  public long getTotalRunCount() {
    return totalRunCount;
  }

  /**
   * Returns the latest run record, or null if there are no runs.
   */
  @Nullable
  public RunRecord getLatestRun() {
    return latestRun;
  }
}

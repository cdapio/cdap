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

package co.cask.cdap.report.proto.summary;

import java.util.List;

/**
 * Represents the summary of a program operation status report in an HTTP response.
 */
public class ReportSummary {
  private final List<NamespaceAggregate> namespaces;
  private final long start;
  private final long end;
  private final List<ArtifactAggregate> artifacts;
  private final DurationStats durations;
  private final StartStats starts;
  private final List<UserAggregate> owners;
  private final List<StartMethodAggregate> startMethods;

  public ReportSummary(List<NamespaceAggregate> namespaces, long start, long end,
                       List<ArtifactAggregate> artifacts,
                       DurationStats durations, StartStats starts, List<UserAggregate> owners,
                       List<StartMethodAggregate> startMethods) {
    this.namespaces = namespaces;
    this.start = start;
    this.end = end;
    this.artifacts = artifacts;
    this.durations = durations;
    this.starts = starts;
    this.owners = owners;
    this.startMethods = startMethods;
  }

  /**
   * @return the number of program runs in each unique namespaces
   */
  public List<NamespaceAggregate> getNamespaces() {
    return namespaces;
  }

  /**
   * @return start time in seconds from {@link co.cask.cdap.report.proto.ReportGenerationRequest#start}
   */
  public Long getStart() {
    return start;
  }

  /**
   * @return end time in seconds from {@link co.cask.cdap.report.proto.ReportGenerationRequest#end}

   */
  public Long getEnd() {
    return end;
  }

  /**
   * @return the number of program runs in each unique parent artifact
   */
  public List<ArtifactAggregate> getArtifacts() {
    return artifacts;
  }

  /**
   * @return the min, max, and average duration of all program runs
   */
  public DurationStats getDurations() {
    return durations;
  }

  /**
   * @return the newest and oldest start time of all program runs
   */
  public StartStats getStarts() {
    return starts;
  }

  /**
   * @return the number of program runs in each unique user who starts the program run
   */
  public List<UserAggregate> getOwners() {
    return owners;
  }

  /**
   * @return the number of program runs of each unique start method
   */
  public List<StartMethodAggregate> getStartMethods() {
    return startMethods;
  }
}

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

package co.cask.cdap.proto.ops;

import java.util.List;
import java.util.Map;

/**
 * Represents the summary of a program operation status report in an HTTP response.
 */
public class ReportSummary {
  private final List<String> namespaces;
  private final long startTs;
  private final long endTs;
  private final Map<String, Integer> programTypeCount;
  private final long minDurationTs;
  private final long maxDurationTs;
  private final long averageDurationTs;
  private final long newestStartedTs;
  private final long oldestStartedTs;
  private final List<ProgramRunOwner> owners;
  private final int startedManually;
  private final int startedByTimeSchedule;
  private final int startedByProgramTrigger;

  public ReportSummary(List<String> namespaces, long startTs, long endTs, Map<String, Integer> programTypeCount,
                       long minDurationTs, long maxDurationTs, long averageDurationTs,
                       long newestStartedTs, long oldestStartedTs, List<ProgramRunOwner> owners,
                       int startedManually, int startedByTimeSchedule, int startedByProgramTrigger) {
    this.namespaces = namespaces;
    this.startTs = startTs;
    this.endTs = endTs;
    this.programTypeCount = programTypeCount;
    this.minDurationTs = minDurationTs;
    this.maxDurationTs = maxDurationTs;
    this.averageDurationTs = averageDurationTs;
    this.newestStartedTs = newestStartedTs;
    this.oldestStartedTs = oldestStartedTs;
    this.owners = owners;
    this.startedManually = startedManually;
    this.startedByTimeSchedule = startedByTimeSchedule;
    this.startedByProgramTrigger = startedByProgramTrigger;
  }

  public List<String> getNamespaces() {
    return namespaces;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public Map<String, Integer> getProgramTypeCount() {
    return programTypeCount;
  }

  public long getMinDurationTs() {
    return minDurationTs;
  }

  public long getMaxDurationTs() {
    return maxDurationTs;
  }

  public long getAverageDurationTs() {
    return averageDurationTs;
  }

  public long getNewestStartedTs() {
    return newestStartedTs;
  }

  public long getOldestStartedTs() {
    return oldestStartedTs;
  }

  public List<ProgramRunOwner> getOwners() {
    return owners;
  }

  public int getStartedManually() {
    return startedManually;
  }

  public int getStartedByTimeSchedule() {
    return startedByTimeSchedule;
  }

  public int getStartedByProgramTrigger() {
    return startedByProgramTrigger;
  }

  public static class ProgramRunOwner {
    private final String user;
    private final int runs;

    public ProgramRunOwner(String user, int runs) {
      this.user = user;
      this.runs = runs;
    }

    public String getUser() {
      return user;
    }

    public int getRuns() {
      return runs;
    }
  }
}

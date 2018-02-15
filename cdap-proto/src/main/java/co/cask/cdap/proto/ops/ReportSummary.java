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

/**
 * Represents the summary of a program operation status report in an HTTP response.
 */
public class ReportSummary {
  private final List<String> namespaces;
  private final long start;
  private final long end;
  private final List<ProgramRunArtifact> artifacts;
  private final DurationStats durations;
  private final StartStats starts;
  private final List<ProgramRunOwner> owners;
  private final List<ProgramRunStartMethod> startMethods;

  public ReportSummary(List<String> namespaces, long start, long end, List<ProgramRunArtifact> artifacts,
                       DurationStats durations, StartStats starts, List<ProgramRunOwner> owners,
                       List<ProgramRunStartMethod> startMethods) {
    this.namespaces = namespaces;
    this.start = start;
    this.end = end;
    this.artifacts = artifacts;
    this.durations = durations;
    this.starts = starts;
    this.owners = owners;
    this.startMethods = startMethods;
  }

  public List<String> getNamespaces() {
    return namespaces;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public List<ProgramRunArtifact> getArtifacts() {
    return artifacts;
  }

  public DurationStats getDurations() {
    return durations;
  }

  public StartStats getStarts() {
    return starts;
  }

  public List<ProgramRunOwner> getOwners() {
    return owners;
  }

  public List<ProgramRunStartMethod> getStartMethods() {
    return startMethods;
  }

  /**
   * Represents the statistics of durations of all program runs.
   */
  public static class DurationStats {
    private long min;
    private long max;
    private double average;

    public DurationStats(long min, long max, double average) {
      this.min = min;
      this.max = max;
      this.average = average;
    }

    public long getMin() {
      return min;
    }

    public long getMax() {
      return max;
    }

    public double getAverage() {
      return average;
    }
  }

  /**
   * Represents the statistics of the start time of all program runs.
   */
  public static class StartStats {
    private long newest;
    private long oldest;

    public StartStats(long newest, long oldest) {
      this.newest = newest;
      this.oldest = oldest;
    }

    public long getNewest() {
      return newest;
    }

    public long getOldest() {
      return oldest;
    }
  }

  /**
   * Represents an aggregate of program runs.
   */
  private abstract static class ProgramRunAggregate {
    private final int runs;

    ProgramRunAggregate(int runs) {
      this.runs = runs;
    }

    public int getRuns() {
      return runs;
    }
  }

  /**
   * Represents an aggregate of program runs by the parent artifact.
   */
  public static class ProgramRunArtifact extends ProgramRunAggregate {
    private final String scope;
    private final String name;
    private final String version;

    public ProgramRunArtifact(String scope, String name, String version, int runs) {
      super(runs);
      this.scope = scope;
      this.name = name;
      this.version = version;
    }

    public String getScope() {
      return scope;
    }

    public String getName() {
      return name;
    }

    public String getVersion() {
      return version;
    }
  }

  /**
   * Represents an aggregate of program runs by the user who starts the program run.
   */
  public static class ProgramRunOwner extends ProgramRunAggregate {
    private final String user;

    public ProgramRunOwner(String user, int runs) {
      super(runs);
      this.user = user;
    }

    public String getUser() {
      return user;
    }
  }

  /**
   * Represents an aggregate of program runs by the start method.
   */
  public static class ProgramRunStartMethod extends ProgramRunAggregate {
    private final String method;

    public ProgramRunStartMethod(String method, int runs) {
      super(runs);
      this.method = method;
    }

    public String getMethod() {
      return method;
    }
  }
}

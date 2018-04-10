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

package co.cask.cdap.report.proto;

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

  public List<NamespaceAggregate> getNamespaces() {
    return namespaces;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public List<ArtifactAggregate> getArtifacts() {
    return artifacts;
  }

  public DurationStats getDurations() {
    return durations;
  }

  public StartStats getStarts() {
    return starts;
  }

  public List<UserAggregate> getOwners() {
    return owners;
  }

  public List<StartMethodAggregate> getStartMethods() {
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

    /**
     * @return the shortest duration
     */
    public long getMin() {
      return min;
    }

    /**
     * @return the longest duration
     */
    public long getMax() {
      return max;
    }

    /**
     * @return the average duration
     */
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

    /**
     * @return number of program runs in the aggregate
     */
    public int getRuns() {
      return runs;
    }
  }

  /**
   * Represents an aggregate of program runs by the namespace.
   */
  public static class NamespaceAggregate extends ProgramRunAggregate {
    private final String namespace;

    public NamespaceAggregate(int runs, String namespace) {
      super(runs);
      this.namespace = namespace;
    }

    /**
     * @return the name of the namespace
     */
    public String getNamespace() {
      return namespace;
    }
  }

  /**
   * Represents an aggregate of program runs by the parent artifact.
   */
  public static class ArtifactAggregate extends ProgramRunAggregate {
    private final String scope;
    private final String name;
    private final String version;

    public ArtifactAggregate(String scope, String name, String version, int runs) {
      super(runs);
      this.scope = scope;
      this.name = name;
      this.version = version;
    }

    /**
     * @return the scope of the artifact
     */
    public String getScope() {
      return scope;
    }

    /**
     * @return the name of the artifact
     */
    public String getName() {
      return name;
    }

    /**
     * @return the version of the artifact
     */
    public String getVersion() {
      return version;
    }
  }

  /**
   * Represents an aggregate of program runs by the user who starts the program run.
   */
  public static class UserAggregate extends ProgramRunAggregate {
    private final String user;

    public UserAggregate(String user, int runs) {
      super(runs);
      this.user = user;
    }

    /**
     * @return the name of the user who started the program run
     */
    public String getUser() {
      return user;
    }
  }

  /**
   * Represents an aggregate of program runs by the start method.
   */
  public static class StartMethodAggregate extends ProgramRunAggregate {
    private final String method;

    public StartMethodAggregate(String method, int runs) {
      super(runs);
      this.method = method;
    }

    /**
     * @return the method of how the program run was started
     */
    public String getMethod() {
      return method;
    }
  }
}

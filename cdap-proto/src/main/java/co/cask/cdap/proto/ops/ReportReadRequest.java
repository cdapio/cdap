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

import co.cask.cdap.proto.ProgramRunStatus;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Represents a request to process a program run report in an HTTP request.
 */
public class ReportReadRequest {
  @Nullable
  private final Filter<String> namespace;
  @Nullable
  private final Filter<String> programName;
  @Nullable
  private final Filter<String> type;
  @Nullable
  private final Filter<ProgramRunStatus> status;
  @Nullable
  private final Sortable<String> startTs;
  @Nullable
  private final Sortable<String> endTs;
  @Nullable
  private final Sortable<String> durationTs;
  @Nullable
  private final Sortable<String> user;
  @Nullable
  private final Filter<String> startMethod;
  @Nullable
  private final String runtimeArgs;
  @Nullable
  private final Sortable<String> memory;
  @Nullable
  private final Sortable<String> numCores;
  @Nullable
  private final Sortable<String> numContainers;
  @Nullable
  private final Sortable<String> numLogWarnings;
  @Nullable
  private final Sortable<String> numLogErrors;
  @Nullable
  private final Sortable<String> numRecordsOut;

  public ReportReadRequest(@Nullable Filter<String> namespace, @Nullable Filter<String> programName,
                           @Nullable Filter<String> type, @Nullable Filter<ProgramRunStatus> status,
                           @Nullable Sortable<String> startTs, @Nullable Sortable<String> endTs,
                           @Nullable Sortable<String> durationTs, @Nullable Sortable<String> user,
                           @Nullable Filter<String> startMethod, @Nullable String runtimeArgs,
                           @Nullable Sortable<String> memory, @Nullable Sortable<String> numCores,
                           @Nullable Sortable<String> numContainers, @Nullable Sortable<String> numLogWarnings,
                           @Nullable Sortable<String> numLogErrors, @Nullable Sortable<String> numRecordsOut) {
    this.namespace = namespace;
    this.programName = programName;
    this.type = type;
    this.status = status;
    this.startTs = startTs;
    this.endTs = endTs;
    this.durationTs = durationTs;
    this.user = user;
    this.startMethod = startMethod;
    this.runtimeArgs = runtimeArgs;
    this.memory = memory;
    this.numCores = numCores;
    this.numContainers = numContainers;
    this.numLogWarnings = numLogWarnings;
    this.numLogErrors = numLogErrors;
    this.numRecordsOut = numRecordsOut;
  }

  @Nullable
  public Filter<String> getNamespace() {
    return namespace;
  }

  @Nullable
  public Filter<String> getProgramName() {
    return programName;
  }

  @Nullable
  public Filter<String> getType() {
    return type;
  }

  @Nullable
  public Filter<ProgramRunStatus> getStatus() {
    return status;
  }

  @Nullable
  public Sortable<String> getStartTs() {
    return startTs;
  }

  @Nullable
  public Sortable<String> getEndTs() {
    return endTs;
  }

  @Nullable
  public Sortable<String> getDurationTs() {
    return durationTs;
  }

  @Nullable
  public Sortable<String> getUser() {
    return user;
  }

  @Nullable
  public Filter<String> getStartMethod() {
    return startMethod;
  }

  @Nullable
  public String getRuntimeArgs() {
    return runtimeArgs;
  }

  @Nullable
  public Sortable<String> getMemory() {
    return memory;
  }

  @Nullable
  public Sortable<String> getNumCores() {
    return numCores;
  }

  @Nullable
  public Sortable<String> getNumContainers() {
    return numContainers;
  }

  @Nullable
  public Sortable<String> getNumLogWarnings() {
    return numLogWarnings;
  }

  @Nullable
  public Sortable<String> getNumLogErrors() {
    return numLogErrors;
  }

  @Nullable
  public Sortable<String> getNumRecordsOut() {
    return numRecordsOut;
  }

  /**
   * Allowed values in a field to be included in the report.
   *
   * @param <T> type of the values
   */
  public static class Filter<T> {
    private final List<T> value;

    public Filter(List<T> value) {
      this.value = value;
    }

    public List<T> getValue() {
      return value;
    }
  }

  /**
   * A class represents the allowed range of values in a field to be included in the report and
   * the order of sorting by this field.
   *
   * @param <T> type of the values
   */
  public static class Sortable<T> {
    private final Range<T> range;
    private final SortBy sortBy;

    public Sortable(@Nullable Range<T> range, @Nullable SortBy sortBy) {
      this.range = range;
      this.sortBy = sortBy;
    }

    public Range<T> getRange() {
      return range;
    }

    public SortBy getSortBy() {
      return sortBy;
    }
  }

  /**
   * Allowed range of values in a field to be included in the report and the order sorting.
   *
   * @param <T>
   */
  public static class Range<T> {
    private final T min;
    private final T max;

    public Range(T min, T max) {
      this.min = min;
      this.max = max;
    }

    public T getMin() {
      return min;
    }

    public T getMax() {
      return max;
    }
  }

  /**
   * The order to sort a field by.
   */
  public enum SortBy {
    ASCENDING,
    DESCENDING
  }
}

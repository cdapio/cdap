/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.proto;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Simplified (filtered) representation of a MapReduce Job.
 */
public class MRJobInfo {
  @Nullable
  private String state;
  @Nullable
  private Long startTime;
  @Nullable
  private Long stopTime;

  private final Float mapProgress;
  private final Float reduceProgress;
  private final Map<String, Long> counters;
  private final List<MRTaskInfo> mapTasks;
  private final List<MRTaskInfo> reduceTasks;
  // If false, the nullable fields in the MRTaskInfo in the mapTasks and reduceTasks will be null.
  private final boolean complete;

  public MRJobInfo(Float mapProcess, Float reduceProgress, Map<String, Long> counters,
                   List<MRTaskInfo> mapTasks, List<MRTaskInfo> reduceTasks, boolean complete) {
    this.mapProgress = mapProcess;
    this.reduceProgress = reduceProgress;
    this.counters = counters;
    this.mapTasks = mapTasks;
    this.reduceTasks = reduceTasks;
    this.complete = complete;
  }

  public void setState(@Nullable String state) {
    this.state = state;
  }

  public void setStartTime(@Nullable Long startTime) {
    this.startTime = startTime;
  }

  public void setStopTime(@Nullable Long stopTime) {
    this.stopTime = stopTime;
  }

  @Nullable
  public String getState() {
    return state;
  }

  @Nullable
  public Long getStartTime() {
    return startTime;
  }

  @Nullable
  public Long getStopTime() {
    return stopTime;
  }

  public Float getMapProgress() {
    return mapProgress;
  }

  public Float getReduceProgress() {
    return reduceProgress;
  }

  public Map<String, Long> getCounters() {
    return counters;
  }

  public List<MRTaskInfo> getMapTasks() {
    return mapTasks;
  }

  public List<MRTaskInfo> getReduceTasks() {
    return reduceTasks;
  }

  public boolean isComplete() {
    return complete;
  }
}

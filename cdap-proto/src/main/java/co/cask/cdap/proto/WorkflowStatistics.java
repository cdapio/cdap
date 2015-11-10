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

/**
 * Class that is used to return the statistics of a workflow
 */
public class WorkflowStatistics {
  private final long startTime;
  private final long endTime;
  private final int runs;
  private final double avgRunTime;
  private final List<PercentileInformation> percentileInformationList;
  private final Map<String, Map<String, String>> nodes;

  public WorkflowStatistics(long startTime, long endTime, int runs, double avgRunTime,
                            List<PercentileInformation> percentileInformationList,
                            Map<String, Map<String, String>> nodes) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.runs = runs;
    this.avgRunTime = avgRunTime;
    this.percentileInformationList = percentileInformationList;
    this.nodes = nodes;
  }

  public List<PercentileInformation> getPercentileInformationList() {
    return percentileInformationList;
  }

  public int getRuns() {
    return runs;
  }

  public double getAvgRunTime() {
    return avgRunTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  /**
   * Sample response of getNodes
   *
   * @return
   * {"FirstMapReduceProgram":{"avgRunTime":"3.6666666666666665",
   * "88.6":"4","95.0":"4","runs":"3","type":"MapReduce"}, "FirstSparkProgram":{"avgRunTime":"3.5",
   * "70.0":"4","95.0":"5","runs":"10","type":"Spark"}}
   *
   */
  public Map<String, Map<String, String>> getNodes() {
    return nodes;
  }
}

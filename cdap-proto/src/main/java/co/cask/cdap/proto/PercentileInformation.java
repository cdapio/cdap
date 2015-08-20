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

/**
 * This class keeps track of the percentiles, and run ids for a particular percentile.
 */
public class PercentileInformation {
  private final double percentile;
  private final long percentileTimeInSeconds;
  private final List<String> runIdsOverPercentile;

  public PercentileInformation(double percentile, long percentileTimeInSeconds, List<String> runIdsOverPercentile) {
    this.percentile = percentile;
    this.percentileTimeInSeconds = percentileTimeInSeconds;
    this.runIdsOverPercentile = runIdsOverPercentile;
  }

  public double getPercentile() {
    return percentile;
  }

  public long getPercentileTimeInSeconds() {
    return percentileTimeInSeconds;
  }

  public List<String> getRunIdsOverPercentile() {
    return runIdsOverPercentile;
  }
}

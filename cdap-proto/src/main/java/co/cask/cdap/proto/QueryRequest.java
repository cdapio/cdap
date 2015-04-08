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
 * Metrics Query Request format
 */
public class QueryRequest {
  /**
   * Format for metrics query in batched queries
   */
    Map<String, String> tags;
    List<String> metrics;
    List<String> groupBy;
    Map<String, String> timeRange;

    public QueryRequest(Map<String, String> tags, List<String> metrics, List<String> groupBy,
                 Map<String, String> timeRange) {
      this.tags = tags;
      this.metrics = metrics;
      this.groupBy = groupBy;
      this.timeRange = timeRange;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    public List<String> getMetrics() {
      return metrics;
    }

    public List<String> getGroupBy() {
      return groupBy;
    }

    public Map<String, String> getTimeRange() {
      return timeRange;
    }
}

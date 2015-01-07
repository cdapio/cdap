/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.metrics.query;

import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.metrics.data.Interpolator;

import java.net.URI;

/**
 * Representing a metric query request.
 */
interface MetricsRequest {

  /**
   * Type of the request.
   */
  enum Type {
    TIME_SERIES,
    SUMMARY,
    AGGREGATE
  }

  enum TimeSeriesResolution {
    SECOND(1),
    MINUTE(60),
    HOUR(3600);

    private int resolution;
    private TimeSeriesResolution(int resolution) {
      this.resolution = resolution;
    }

    public int getResolution() {
      return resolution;
    }
  }

  URI getRequestURI();

  String getContextPrefix();

  String getRunId();

  String getMetricPrefix();

  String getTagPrefix();

  long getStartTime();

  long getEndTime();

  Type getType();

  int getTimeSeriesResolution();

  int getCount();

  Interpolator getInterpolator();

  MetricsScope getScope();
}

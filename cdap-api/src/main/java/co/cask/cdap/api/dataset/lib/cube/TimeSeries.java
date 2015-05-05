/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib.cube;

import co.cask.cdap.api.annotation.Beta;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a single time series defined by a measure name and set of dimension values.
 */
@Beta
public final class TimeSeries {
  private final String measureName;
  private final Map<String, String> dimensionValues;
  private final List<TimeValue> timeValues;

  public TimeSeries(String measureName, Map<String, String> dimensionValues, List<TimeValue> timeValues) {
    this.measureName = measureName;
    this.dimensionValues = Collections.unmodifiableMap(new HashMap<String, String>(dimensionValues));
    this.timeValues = Collections.unmodifiableList(timeValues);
  }

  public String getMeasureName() {
    return measureName;
  }

  public Map<String, String> getDimensionValues() {
    return dimensionValues;
  }

  public List<TimeValue> getTimeValues() {
    return timeValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimeSeries that = (TimeSeries) o;

    return Objects.equal(measureName, that.measureName) &&
      Objects.equal(dimensionValues, that.dimensionValues) &&
      Objects.equal(timeValues, that.timeValues);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(measureName, dimensionValues, timeValues);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("TimeSeries");
    sb.append("{measureName='").append(measureName).append('\'');
    sb.append(", dimensionValues=").append(dimensionValues);
    sb.append(", timeValues=").append(timeValues);
    sb.append('}');
    return sb.toString();
  }
}

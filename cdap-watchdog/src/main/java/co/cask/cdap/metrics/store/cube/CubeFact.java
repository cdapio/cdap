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

package co.cask.cdap.metrics.store.cube;

import co.cask.cdap.metrics.store.timeseries.MeasureType;
import co.cask.cdap.metrics.store.timeseries.TimeValue;

import java.util.Map;

/**
 * Time-based measurement with associated tags (dimensions) to be stored in {@link Cube}.
 * <p/>
 * See also {@link Cube#add(CubeFact)}.
 */
public class CubeFact {
  private final Map<String, String> tagValues;
  private final MeasureType measureType;
  private final String measureName;
  private final TimeValue timeValue;

  public CubeFact(Map<String, String> tagValues, MeasureType measureType, String measureName, TimeValue timeValue) {
    this.tagValues = tagValues;
    this.measureType = measureType;
    this.measureName = measureName;
    this.timeValue = timeValue;
  }

  public Map<String, String> getTagValues() {
    return tagValues;
  }

  public MeasureType getMeasureType() {
    return measureType;
  }

  public String getMeasureName() {
    return measureName;
  }

  public TimeValue getTimeValue() {
    return timeValue;
  }
}

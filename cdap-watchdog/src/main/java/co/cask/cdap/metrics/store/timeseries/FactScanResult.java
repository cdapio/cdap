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

package co.cask.cdap.metrics.store.timeseries;

import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.api.metrics.TimeValue;

import java.util.Iterator;
import java.util.List;

/**
 * An single result item returned by {@link FactScanner}.
 */
public final class FactScanResult implements Iterable<TimeValue> {
  private final String measureName;
  private final List<TagValue> tagValues;
  private final Iterable<TimeValue> timeValues;

  public FactScanResult(String measureName, List<TagValue> tagValues, Iterable<TimeValue> timeValues) {
    this.measureName = measureName;
    this.tagValues = tagValues;
    this.timeValues = timeValues;
  }

  public String getMeasureName() {
    return measureName;
  }

  public List<TagValue> getTagValues() {
    return tagValues;
  }

  @Override
  public Iterator<TimeValue> iterator() {
    return timeValues.iterator();
  }
}

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

package co.cask.cdap.data2.dataset2.lib.timeseries;

import co.cask.cdap.api.dataset.lib.cube.Measurement;
import co.cask.cdap.api.dataset.lib.cube.TagValue;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents measure in time with tags assigned to it
 */
public final class Fact {
  /** in seconds */
  private final long timestamp;
  private final List<TagValue> tagValues;
  private final Collection<Measurement> measurements;

  public Fact(long timestamp, List<TagValue> tagValues, Collection<Measurement> measurements) {
    this.timestamp = timestamp;
    this.tagValues = ImmutableList.copyOf(tagValues);
    this.measurements = measurements;
  }

  public Fact(long timestamp, List<TagValue> tagValues, Measurement measurement) {
    this.timestamp = timestamp;
    this.tagValues = ImmutableList.copyOf(tagValues);
    this.measurements = ImmutableList.of(measurement);
  }

  public List<TagValue> getTagValues() {
    return Collections.unmodifiableList(tagValues);
  }

  public Collection<Measurement> getMeasurements() {
    return Collections.unmodifiableCollection(measurements);
  }

  public long getTimestamp() {
    return timestamp;
  }
}

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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Time-based measurement with associated tags (dimensions) to be stored in {@link Cube}.
 * <p/>
 * See also {@link Cube#add(CubeFact)}.
 */
@Beta
public class CubeFact {
  private final long timestamp;
  private final Map<String, String> tags;
  private final Collection<Measurement> measurements;

  /**
   * Creates an instance of {@link CubeFact} with no tags and no measurements.
   * <p/>
   * After creation, you can add tags e.g. via {@link #addTag(String, String)}
   * and add measurements e.g. via {@link #addMeasurement(String, MeasureType, long)}.
   *
   * @param timestamp timestamp (epoch in sec) of the measurements
   */
  public CubeFact(long timestamp) {
    this.tags = new HashMap<String, String>();
    this.measurements = new LinkedList<Measurement>();
    this.timestamp = timestamp;
  }

  /**
   * Adds tag to this {@link CubeFact}.
   * @param name name of the tag
   * @param value value of the tag
   * @return this {@link CubeFact}
   */
  public CubeFact addTag(String name, String value) {
    tags.put(name, value);
    return this;
  }

  /**
   * Adds multiple tags to this {@link CubeFact}.
   * @param tags tags to add
   * @return this {@link CubeFact}
   */
  public CubeFact addTags(Map<String, String> tags) {
    this.tags.putAll(tags);
    return this;
  }

  /**
   * Adds a {@link Measurement} to this {@link CubeFact}.
   * @param name name of the measurement to add
   * @param type type of the measurement to add
   * @param value value of the measurement to add
   * @return this {@link CubeFact}
   */
  public CubeFact addMeasurement(String name, MeasureType type, long value) {
    measurements.add(new Measurement(name, type, value));
    return this;
  }

  /**
   * Adds a {@link Measurement} to this {@link CubeFact}.
   * @param measurement a {@link Measurement} to add
   * @return this {@link CubeFact}
   */
  public CubeFact addMeasurement(Measurement measurement) {
    measurements.add(measurement);
    return this;
  }

  /**
   * Adds multiple {@link Measurement}s to this {@link CubeFact}
   * @param measurements {@link Measurement}s to add
   * @return this {@link CubeFact}
   */
  public CubeFact addMeasurements(Collection<Measurement> measurements) {
    this.measurements.addAll(measurements);
    return this;
  }

  /**
   * @return timestamp of this {@link CubeFact}
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return tags of this {@link CubeFact}
   */
  public Map<String, String> getTags() {
    return Collections.unmodifiableMap(tags);
  }

  /**
   * @return {@link Measurement}s of this {@link CubeFact}
   */
  public Collection<Measurement> getMeasurements() {
    return Collections.unmodifiableCollection(measurements);
  }
}

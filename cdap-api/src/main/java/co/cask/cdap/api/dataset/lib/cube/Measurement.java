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

/**
 * A single measurement in the {@link CubeFact}.
 */
public class Measurement {
  private final String name;
  private final MeasureType type;
  private final long value;

  /**
   * Creates a {@link Measurement}.
   * @param name name of the measurement
   * @param type type of the measurement
   * @param value value of the measurement
   */
  public Measurement(String name, MeasureType type, long value) {
    this.name = name;
    this.type = type;
    this.value = value;
  }

  /**
   * Creates a {@link Measurement}.
   * @param measurement existing measurement
   * @param name name of the measurement
   */
  public Measurement(Measurement measurement, String name) {
    this.name = name;
    this.type = measurement.getType();
    this.value = measurement.getValue();
  }

  /**
   * @return name of this {@link Measurement}
   */
  public String getName() {
    return name;
  }

  /**
   * @return type of this {@link Measurement}
   */
  public MeasureType getType() {
    return type;
  }

  /**
   * @return value of this {@link Measurement}
   */
  public long getValue() {
    return value;
  }
}

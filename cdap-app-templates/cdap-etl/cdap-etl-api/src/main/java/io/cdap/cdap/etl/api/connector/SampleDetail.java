/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.api.connector;

import io.cdap.cdap.api.data.format.StructuredRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The result for the sample request
 */
public class SampleDetail {
  private final List<StructuredRecord> sample;
  private final Map<String, String> properties;

  private SampleDetail(List<StructuredRecord> sample, Map<String, String> properties) {
    this.sample = sample;
    this.properties = properties;
  }

  public List<StructuredRecord> getSample() {
    return sample;
  }

  /**
   * Get the all the properties used to generate this sample, these properties can be directly used by a source/sink
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SampleDetail that = (SampleDetail) o;
    return Objects.equals(sample, that.sample) &&
             Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sample, properties);
  }

  /**
   * Get the builder to build this object
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link SampleDetail}
   */
  public static class Builder {
    private List<StructuredRecord> sample;
    private Map<String, String> properties;

    public Builder() {
      this.sample = new ArrayList<>();
      this.properties = new HashMap<>();
    }

    public Builder setSample(List<StructuredRecord> sample) {
      this.sample.clear();
      this.sample.addAll(sample);
      return this;
    }

    public Builder setProperties(Map<String, String> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return this;
    }

    public SampleDetail build() {
      return new SampleDetail(sample, properties);
    }
  }
}

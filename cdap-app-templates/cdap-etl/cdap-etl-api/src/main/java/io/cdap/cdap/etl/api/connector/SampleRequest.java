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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Request for sampling operations
 */
public class SampleRequest {
  private final String path;
  private final int limit;
  private final Map<String, String> properties;

  private SampleRequest(@Nullable String path, int limit, Map<String, String> properties) {
    this.path = path;
    this.limit = limit;
    this.properties = properties;
  }

  /**
   * Get the entity path for the sample request, if the path is null, that means the properties contains
   * all the path related configs required for the sampling
   */
  @Nullable
  public String getPath() {
    return path;
  }

  public int getLimit() {
    return limit;
  }

  public Map<String, String> getProperties() {
    // might be null because of gson
    return properties == null ? Collections.emptyMap() : properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SampleRequest that = (SampleRequest) o;
    return limit == that.limit &&
             Objects.equals(path, that.path) &&
             Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, limit, properties);
  }

  /**
   * Get the builder to build this object
   */
  public static Builder builder(int limit) {
    return new Builder(limit);
  }

  /**
   * Builder for {@link SampleRequest}
   */
  public static class Builder {
    private String path;
    private int limit;
    private Map<String, String> properties;

    public Builder(int limit) {
      this.limit = limit;
      this.properties = new HashMap<>();
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setLimit(Integer limit) {
      this.limit = limit;
      return this;
    }

    public Builder setProperties(Map<String, String> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return this;
    }

    public SampleRequest build() {
      return new SampleRequest(path, limit, properties);
    }
  }
}

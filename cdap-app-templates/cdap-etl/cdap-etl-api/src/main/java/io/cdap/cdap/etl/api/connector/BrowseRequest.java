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

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Request used for browse operations
 */
public class BrowseRequest {
  private final String path;
  private final Integer limit;

  private BrowseRequest(String path, @Nullable Integer limit) {
    this.path = path;
    this.limit = limit;
  }

  public String getPath() {
    return path;
  }

  /**
   * Return the max number of results to retrieve, if null or less than or equal to 0,
   * the connector will fetch all the results
   */
  @Nullable
  public Integer getLimit() {
    return limit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BrowseRequest that = (BrowseRequest) o;
    return Objects.equals(path, that.path) &&
             Objects.equals(limit, that.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, limit);
  }

  /**
   * Get the builder to build this object
   */
  public static Builder builder(String path) {
    return new Builder(path);
  }

  /**
   * Builder for {@link BrowseRequest}
   */
  public static class Builder {
    private String path;
    private Integer limit;

    public Builder(String path) {
      this.path = path;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setLimit(Integer limit) {
      this.limit = limit;
      return this;
    }

    public BrowseRequest build() {
      return new BrowseRequest(path, limit);
    }
  }
}

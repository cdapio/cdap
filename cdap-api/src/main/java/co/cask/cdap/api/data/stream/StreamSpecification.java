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

package co.cask.cdap.api.data.stream;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Specification for {@link Stream}.
 */
public final class StreamSpecification {
  private final String name;
  private final String description;

  private StreamSpecification(String name, @Nullable String description) {
    this.name = name;
    this.description = description;
  }

  /**
   * Returns the name of the Stream.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the description of the Stream.
   */
  @Nullable
  public String getDescription() {
    return description;
  }

  /**
  * {@code StreamSpecification} builder used to build specification of stream.
  */
  public static final class Builder {
    private String name;
    private String description;

    /**
     * Adds name parameter to Streams.
     * @param name stream name
     * @return Builder instance
     */
    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    /**
     * Create {@code StreamSpecification}.
     * @return Instance of {@code StreamSpecification}
     */
    public StreamSpecification create() {
      return new StreamSpecification(name, description);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamSpecification that = (StreamSpecification) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description);
  }
}

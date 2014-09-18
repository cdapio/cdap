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

/**
 * Specification for {@link Stream}.
 */
public final class StreamSpecification {
  private final String name;

  private StreamSpecification(final String name) {
    this.name = name;
  }

  /**
   * Returns the name of the Stream.
   */
  public String getName() {
    return name;
  }

 /**
  * {@code StreamSpecification} builder used to build specification of stream.
  */
  public static final class Builder {
    private String name;

    /**
     * Adds name parameter to Streams.
     * @param name stream name
     * @return Builder instance
     */
    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    /**
     * Create {@code StreamSpecification}.
     * @return Instance of {@code StreamSpecification}
     */
    public StreamSpecification create() {
      StreamSpecification specification = new StreamSpecification(name);
      return specification;
    }
  }
}

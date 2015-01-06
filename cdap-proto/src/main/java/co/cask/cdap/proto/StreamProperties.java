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
package co.cask.cdap.proto;

import co.cask.cdap.internal.specification.FormatSpecification;
import com.google.common.base.Objects;

/**
 * Represents the properties of a stream.
 */
public final class StreamProperties {

  private final String name;
  private final Long ttl;
  private final FormatSpecification format;

  public StreamProperties(String name, long ttl, FormatSpecification format) {
    this.name = name;
    this.ttl = ttl;
    this.format = format;
  }

  /**
   * @return Name of the stream.
   */
  public String getName() {
    return name;
  }

  /**
   * @return The time to live in seconds for events in this stream.
   */
  public Long getTTL() {
    return ttl;
  }

  /**
   * @return The format specification for the stream.
   */
  public FormatSpecification getFormat() {
    return format;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamProperties)) {
      return false;
    }

    StreamProperties that = (StreamProperties) o;

    return Objects.equal(name, that.name) &&
      Objects.equal(ttl, that.ttl) &&
      Objects.equal(format, that.format);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, ttl, format);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("ttl", ttl)
      .add("format", format)
      .toString();
  }
}

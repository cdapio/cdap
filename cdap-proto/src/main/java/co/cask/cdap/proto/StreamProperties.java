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

import co.cask.cdap.api.data.format.FormatSpecification;
import com.google.common.base.Objects;

/**
 * Represents the properties of a stream.
 */
public class StreamProperties {

  private final Id.Stream streamId;
  private final Long ttl;
  private final FormatSpecification format;
  private final Integer threshold;

  public StreamProperties(Id.Stream streamId, Long ttl, FormatSpecification format, Integer threshold) {
    this.streamId = streamId;
    this.ttl = ttl;
    this.format = format;
    this.threshold = threshold;
  }

  /**
   * @return Name of the stream.
   */
  public Id.Stream getStreamId() {
    return streamId;
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

  /**
   *
   * @return The threshold of the stream
   */
  public Integer getThreshold() {
    return threshold;
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

    return Objects.equal(streamId, that.streamId) &&
      Objects.equal(ttl, that.ttl) &&
      Objects.equal(format, that.format) &
      Objects.equal(threshold, that.threshold);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId, ttl, format, threshold);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("ttl", ttl)
      .add("format", format)
      .add("threshold", threshold)
      .toString();
  }
}

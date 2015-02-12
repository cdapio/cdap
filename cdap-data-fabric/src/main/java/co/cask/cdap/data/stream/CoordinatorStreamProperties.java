/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamProperties;
import com.google.common.base.Objects;

/**
 * This class carries stream properties used for coordination purpose.
 */
public class CoordinatorStreamProperties extends StreamProperties {

  private final Integer generation;

  public CoordinatorStreamProperties(Id.Stream streamId, Long ttl,
                                     FormatSpecification format, Integer threshold, Integer generation) {
    super(streamId, ttl, format, threshold);
    this.generation = generation;
  }

  public Integer getGeneration() {
    return generation;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", getStreamId())
      .add("ttl", getTTL())
      .add("format", getFormat())
      .add("threshold", getThreshold())
      .add("generation", getGeneration())
      .toString();
  }
}

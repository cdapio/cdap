/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import com.google.gson.annotations.SerializedName;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the properties of a stream.
 */
public class StreamProperties {

  private final Long ttl;
  private final FormatSpecification format;

  @SerializedName("notification.threshold.mb")
  private final Integer notificationThresholdMB;
  private final String description;

  public StreamProperties(Long ttl, FormatSpecification format, Integer notificationThresholdMB) {
    this(ttl, format, notificationThresholdMB, null);
  }

  public StreamProperties(Long ttl, FormatSpecification format, Integer notificationThresholdMB,
                          @Nullable String description) {
    this.ttl = ttl;
    this.format = format;
    this.notificationThresholdMB = notificationThresholdMB;
    this.description = description;
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
   * @return The notification threshold of the stream
   */
  public Integer getNotificationThresholdMB() {
    return notificationThresholdMB;
  }

  /**
   * @return The description of the stream
   */
  @Nullable
  public String getDescription() {
    return description;
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

    return Objects.equals(ttl, that.ttl) &&
      Objects.equals(format, that.format) &
      Objects.equals(notificationThresholdMB, that.notificationThresholdMB) &
      Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ttl, format, notificationThresholdMB, description);
  }

  @Override
  public String toString() {
    return "StreamProperties{" +
      "ttl=" + ttl +
      ", format=" + format +
      ", notificationThresholdMB=" + notificationThresholdMB +
      ", description=" + description +
      '}';
  }
}

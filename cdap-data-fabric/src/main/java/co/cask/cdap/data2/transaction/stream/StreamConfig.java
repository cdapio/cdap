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
package co.cask.cdap.data2.transaction.stream;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import org.apache.twill.filesystem.Location;

import java.util.Collections;

/**
 * Represents the configuration of a stream. This class needs to be GSON serializable.
 */
public final class StreamConfig {

  public static final FormatSpecification DEFAULT_STREAM_FORMAT =
    new FormatSpecification(
      Formats.TEXT,
      Schema.recordOf("stringBody", Schema.Field.of("body", Schema.of(Schema.Type.STRING))),
      Collections.<String, String>emptyMap());

  private final transient Id.Stream streamId;
  private final long partitionDuration;
  private final long indexInterval;
  private final long ttl;
  private final FormatSpecification format;
  private final int notificationThresholdMB;

  private final transient Location location;

  public StreamConfig(Id.Stream streamId, long partitionDuration, long indexInterval, long ttl,
                      Location location, FormatSpecification format, int notificationThresholdMB) {
    this.streamId = streamId;
    this.partitionDuration = partitionDuration;
    this.indexInterval = indexInterval;
    this.ttl = ttl;
    this.location = location;
    this.notificationThresholdMB = notificationThresholdMB;
    this.format = format;
  }

  /**
   * @return Id of the stream.
   */
  public Id.Stream getStreamId() {
    return streamId;
  }

  /**
   * @return The duration in milliseconds that one partition in this stream contains.
   */
  public long getPartitionDuration() {
    return partitionDuration;
  }

  /**
   * @return The time interval in milliseconds that a new index entry would be created in the stream.
   */
  public long getIndexInterval() {
    return indexInterval;
  }

  /**
   * @return The time to live in milliseconds for events in this stream.
   */
  public long getTTL() {
    return ttl;
  }

  /**
   * @return The location of the stream.
   */
  public Location getLocation() {
    return location;
  }

  /**
   * @return The format of the stream body.
   */
  public FormatSpecification getFormat() {
    return Objects.firstNonNull(format, DEFAULT_STREAM_FORMAT);
  }

  /**
   * @return The threshold of data, in MB, that the stream has to ingest for a notification to be sent.
   */
  public int getNotificationThresholdMB() {
    return notificationThresholdMB;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("duration", partitionDuration)
      .add("indexInterval", indexInterval)
      .add("ttl", ttl)
      .add("location", location.toURI())
      .add("format", format)
      .add("notificationThresholdMB", notificationThresholdMB)
      .toString();
  }

  public static Builder builder(StreamConfig config) {
    return new Builder(config);
  }

  /**
   * A builder to help building instance of {@link StreamConfig}.
   */
  public static final class Builder {

    private final StreamConfig config;
    private Long ttl;
    private FormatSpecification formatSpec;
    private Integer notificationThreshold;

    private Builder(StreamConfig config) {
      this.config = config;
    }

    public void setTTL(long ttl) {
      this.ttl = ttl;
    }

    public void setFormatSpec(FormatSpecification formatSpec) {
      this.formatSpec = formatSpec;
    }

    public void setNotificationThreshold(int notificationThreshold) {
      this.notificationThreshold = notificationThreshold;
    }

    public StreamConfig build() {
      return new StreamConfig(config.getStreamId(), config.getPartitionDuration(), config.getIndexInterval(),
                              Objects.firstNonNull(ttl, config.getTTL()),
                              config.getLocation(),
                              Objects.firstNonNull(formatSpec, config.getFormat()),
                              Objects.firstNonNull(notificationThreshold, config.getNotificationThresholdMB()));
    }
  }
}

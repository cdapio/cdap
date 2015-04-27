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

package co.cask.cdap.logging.read;

import com.google.common.base.Objects;

/**
 * Boundary of a log read request.
 */
public class ReadRange {
  public static final ReadRange LATEST = new ReadRange(-1, Long.MAX_VALUE, LogOffset.LATEST_KAFKA_OFFSET);

  private final long fromMillis;
  private final long toMillis;
  private final long kafkaOffset;

  public ReadRange(long fromMillis, long toMillis, long kafkaOffset) {
    this.fromMillis = fromMillis;
    this.toMillis = toMillis;
    this.kafkaOffset = kafkaOffset;
  }

  public long getFromMillis() {
    return fromMillis;
  }

  public long getToMillis() {
    return toMillis;
  }

  public long getKafkaOffset() {
    return kafkaOffset;
  }

  public static ReadRange createFromRange(LogOffset logOffset) {
    if (logOffset == LogOffset.LATEST_OFFSET) {
      return ReadRange.LATEST;
    }
    return new ReadRange(logOffset.getTime(), Long.MAX_VALUE, logOffset.getKafkaOffset());
  }

  public static ReadRange createToRange(LogOffset logOffset) {
    if (logOffset == LogOffset.LATEST_OFFSET) {
      return ReadRange.LATEST;
    }
    return new ReadRange(-1, logOffset.getTime(), logOffset.getKafkaOffset());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("fromMillis", fromMillis)
      .add("toMillis", toMillis)
      .add("kafkaOffset", kafkaOffset)
      .toString();
  }
}

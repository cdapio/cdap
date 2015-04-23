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
 * Represents log offset containing Kafka offset and time of logging event.
 */
public class LogOffset {
  public static final long LATEST_KAFKA_OFFSET = -1;
  public static final LogOffset LATEST_OFFSET = new LogOffset(-1, LATEST_KAFKA_OFFSET);
  public static final long INVALID_KAFKA_OFFSET = -10000;

  private final long kafkaOffset;
  private final long time;

  public LogOffset(long kafkaOffset, long time) {
    this.kafkaOffset = kafkaOffset;
    this.time = time;
  }

  public long getKafkaOffset() {
    return kafkaOffset;
  }

  public long getTime() {
    return time;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("kafkaOffset", kafkaOffset)
      .add("time", time)
      .toString();
  }
}

/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.meta;

/**
 * Kafka offset for log processing.
 */
public class KafkaOffset implements Comparable<KafkaOffset> {
  private final long nextOffset;
  private final long nextEventTime;

  /**
   * Kafka offset containing nextOffset and nextEventTime.
   */
  public KafkaOffset(long nextOffset, long nextEventTime) {
    this.nextOffset = nextOffset;
    this.nextEventTime = nextEventTime;
  }

  /**
   * Returns next offset.
   */
  public long getNextOffset() {
    return nextOffset;
  }

  /**
   * Returns next event time.
   */
  public long getNextEventTime() {
    return nextEventTime;
  }

  @Override
  public String toString() {
    return "KafkaOffset{" +
      "nextOffset=" + nextOffset +
      ", nextEventTime=" + nextEventTime +
      '}';
  }

  @Override
  public int compareTo(KafkaOffset o) {
    return Long.compare(this.nextOffset, o.nextOffset);
  }
}

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

package io.cdap.cdap.logging.kafka;

import java.nio.ByteBuffer;

/**
 * Represents a Kafka message.
 */
public final class KafkaMessage {

  private final ByteBuffer byteBuffer;
  private final long offset;

  public KafkaMessage(ByteBuffer byteBuffer, long offset) {
    this.byteBuffer = byteBuffer;
    this.offset = offset;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public long getOffset() {
    return offset;
  }
}

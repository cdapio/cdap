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

import co.cask.cdap.logging.pipeline.kafka.KafkaLogProcessorPipeline;
import co.cask.cdap.spi.data.transaction.TransactionRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Kafka checkpoint manager for {@link KafkaLogProcessorPipeline}.
 */
public class KafkaCheckpointManager extends AbstractCheckpointManager<KafkaOffset> {

  public KafkaCheckpointManager(TransactionRunner transactionRunner, String prefix) {
    super(transactionRunner, prefix);
  }

  @Override
  protected byte[] serializeCheckpoint(Checkpoint<KafkaOffset> checkpoint) throws IOException {
    KafkaOffset offset = checkpoint.getOffset();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(bos)) {
      dos.writeLong(offset.getNextOffset());
      dos.writeLong(offset.getNextEventTime());
      dos.writeLong(checkpoint.getMaxEventTime());
    }
    return bos.toByteArray();
  }

  @Override
  protected Checkpoint<KafkaOffset> deserializeOffset(@Nullable byte[] checkpoint) throws IOException {
    if (checkpoint == null) {
      return new Checkpoint<>(new KafkaOffset(-1, -1), -1);
    }

    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(checkpoint))) {
      long nextOffset = dis.readLong();
      long nextEventTime = dis.readLong();
      long maxEventTime = dis.readLong();
      return new Checkpoint<>(new KafkaOffset(nextOffset, nextEventTime), maxEventTime);
    }
  }
}

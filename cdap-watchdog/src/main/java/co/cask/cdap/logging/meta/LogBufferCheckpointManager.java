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

import co.cask.cdap.logging.logbuffer.LogBufferFileOffset;
import co.cask.cdap.logging.pipeline.logbuffer.LogBufferProcessorPipeline;
import co.cask.cdap.spi.data.transaction.TransactionRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Checkpoint manager for {@link LogBufferProcessorPipeline}.
 */
public class LogBufferCheckpointManager extends AbstractCheckpointManager<LogBufferFileOffset> {

  public LogBufferCheckpointManager(TransactionRunner transactionRunner, String prefix) {
    super(transactionRunner, prefix);
  }

  @Override
  protected byte[] serializeCheckpoint(Checkpoint<LogBufferFileOffset> checkpoint) throws IOException {
    LogBufferFileOffset offset = checkpoint.getOffset();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(bos)) {
      dos.writeLong(offset.getFileId());
      dos.writeLong(offset.getFilePos());
      dos.writeLong(checkpoint.getMaxEventTime());
    }
    return bos.toByteArray();
  }

  @Override
  protected Checkpoint<LogBufferFileOffset> deserializeOffset(byte[] checkpoint) throws IOException {
    if (checkpoint == null) {
      return new Checkpoint<>(new LogBufferFileOffset(-1, -1), -1);
    }
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(checkpoint))) {
      long fileId = dis.readLong();
      long filePos = dis.readLong();
      long maxEventTime = dis.readLong();
      return new Checkpoint<>(new LogBufferFileOffset(fileId, filePos), maxEventTime);
    }
  }
}

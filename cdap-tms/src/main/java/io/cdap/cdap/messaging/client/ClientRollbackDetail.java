/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.client;

import co.cask.cdap.messaging.RollbackDetail;
import co.cask.cdap.messaging.Schemas;
import com.google.common.base.Throwables;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Client side implementation of {@link RollbackDetail}. It retains the original encoded bytes as it is most likely
 * being used instead of the decoded form. The decoding happens on demand when the actual information is needed.
 * However, the need for decoding should be rare and is not needed in normal operation.
 */
final class ClientRollbackDetail implements RollbackDetail {

  private final byte[] encoded;
  private GenericRecord decoded;

  ClientRollbackDetail(byte[] encoded) {
    this.encoded = encoded;
  }

  /**
   * Returns the information contained in this class in the original encoded form as responded from the server.
   */
  byte[] getEncoded() {
    return encoded;
  }

  @Override
  public long getTransactionWritePointer() {
    return (Long) getDecoded().get("transactionWritePointer");
  }

  @Override
  public long getStartTimestamp() {
    return (Long) ((GenericRecord) getDecoded().get("rollbackRange")).get("startTimestamp");
  }

  @Override
  public int getStartSequenceId() {
    return (Integer) ((GenericRecord) getDecoded().get("rollbackRange")).get("startSequenceId");
  }

  @Override
  public long getEndTimestamp() {
    return (Long) ((GenericRecord) getDecoded().get("rollbackRange")).get("endTimestamp");
  }

  @Override
  public int getEndSequenceId() {
    return (Integer) ((GenericRecord) getDecoded().get("rollbackRange")).get("endSequenceId");
  }

  @Override
  public String toString() {
    return "ClientRollbackDetail{" + getDecoded() + '}';
  }

  private synchronized GenericRecord getDecoded() {
    if (decoded != null) {
      return decoded;
    }

    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(encoded), null);
    try {
      decoded = new GenericDatumReader<GenericRecord>(Schemas.V1.PublishResponse.SCHEMA).read(null, decoder);
      return decoded;
    } catch (IOException e) {
      // This shouldn't happen, otherwise the server and client is not compatible
      throw Throwables.propagate(e);
    }
  }
}

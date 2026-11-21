/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.messaging.client;

import io.cdap.cdap.messaging.Schemas;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ClientRollbackDetail}.
 */
public class ClientRollbackDetailTest {

  @Test
  public void testValidDecodeAndAccessors() throws IOException {
    Schema schema = Schemas.V1.PublishResponse.SCHEMA;
    GenericRecord record = new GenericData.Record(schema);
    record.put("transactionWritePointer", 42L);

    GenericRecord range = new GenericData.Record(schema.getField("rollbackRange").schema());
    range.put("startTimestamp", 1000L);
    range.put("startSequenceId", 3);
    range.put("endTimestamp", 2000L);
    range.put("endSequenceId", 9);
    record.put("rollbackRange", range);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();

    byte[] encoded = bos.toByteArray();
    ClientRollbackDetail detail = new ClientRollbackDetail(encoded);
    Assert.assertSame(encoded, detail.getEncoded());
    Assert.assertEquals(42L, detail.getTransactionWritePointer());
    Assert.assertEquals(1000L, detail.getStartTimestamp());
    Assert.assertEquals(3, detail.getStartSequenceId());
    Assert.assertEquals(2000L, detail.getEndTimestamp());
    Assert.assertEquals(9, detail.getEndSequenceId());
    Assert.assertTrue(detail.toString().startsWith("ClientRollbackDetail{"));
  }

  @Test
  public void testCorruptedPayloadPropagatesIoExceptionAsRuntimeException() {
    ClientRollbackDetail detail = new ClientRollbackDetail(new byte[0]);
    try {
      detail.getTransactionWritePointer();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException e) {
      Assert.assertTrue("Expected IOException cause, got: " + e.getCause(),
          e.getCause() instanceof IOException);
    }
  }
}

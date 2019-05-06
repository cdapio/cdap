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

package io.cdap.cdap.etl.batch;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 */
public class StructuredRecordWritableTest {

  @Test
  public void testNonAsciiString() throws IOException {
    Schema schema = Schema.recordOf("rec", Schema.Field.of("x", Schema.of(Schema.Type.STRING)));
    StructuredRecord record = StructuredRecord.builder(schema).set("x", "идыло").build();

    StructuredRecordWritable writableOut = new StructuredRecordWritable(record);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutput output = new DataOutputStream(os);
    writableOut.write(output);
    os.flush();

    StructuredRecordWritable writableIn = new StructuredRecordWritable();
    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    DataInput input = new DataInputStream(is);
    writableIn.readFields(input);

    Assert.assertEquals(writableIn.get(), record);
  }

  @Test
  public void testComparison() {
    Schema schema = Schema.recordOf("l", Schema.Field.of("l", Schema.of(Schema.Type.LONG)));
    StructuredRecord record1 = StructuredRecord.builder(schema).set("l", 0L).build();
    StructuredRecord record2 = StructuredRecord.builder(schema).set("l", -1L).build();
    StructuredRecordWritable writable1 = new StructuredRecordWritable(record1);
    StructuredRecordWritable writable2 = new StructuredRecordWritable(record2);
    Assert.assertNotEquals(0, writable1.compareTo(writable2));
    Assert.assertNotEquals(writable1, writable2);
  }
}

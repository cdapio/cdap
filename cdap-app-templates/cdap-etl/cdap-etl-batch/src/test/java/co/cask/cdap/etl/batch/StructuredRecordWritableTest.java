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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
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
}

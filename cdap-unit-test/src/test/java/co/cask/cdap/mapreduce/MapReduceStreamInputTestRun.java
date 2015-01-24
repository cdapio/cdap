/*
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

package co.cask.cdap.mapreduce;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Test reading from a stream with map reduce with schema in stream
 */
public class MapReduceStreamInputTestRun extends TestFrameworkTestBase {

  @Test
  public void test() throws Exception {

    ApplicationManager applicationManager = deployApplication(AppWithMapReduceUsingStream.class);
    Schema schema = new Schema.Parser().parse(AppWithMapReduceUsingStream.SCHEMA.toString());
    StreamWriter streamWriter = applicationManager.getStreamWriter("mrStream");
    streamWriter.send(createEvent(schema, "YHOO", 100, 10.0f));
    streamWriter.send(createEvent(schema, "YHOO", 10, 10.1f));
    streamWriter.send(createEvent(schema, "YHOO", 13, 9.9f));
    float yhooTotal = 100 * 10.0f + 10 * 10.1f + 13 * 9.9f;
    streamWriter.send(createEvent(schema, "AAPL", 5, 300.0f));
    streamWriter.send(createEvent(schema, "AAPL", 3, 298.34f));
    streamWriter.send(createEvent(schema, "AAPL", 50, 305.23f));
    streamWriter.send(createEvent(schema, "AAPL", 1000, 284.13f));
    float aaplTotal = 5 * 300.0f + 3 * 298.34f + 50 * 305.23f + 1000 * 284.13f;

    MapReduceManager mrManager = applicationManager.startMapReduce("BodyTracker");
    mrManager.waitForFinish(180, TimeUnit.SECONDS);

    KeyValueTable pricesDS = (KeyValueTable) getDataset("prices").get();
    float yhooVal = Bytes.toFloat(pricesDS.read(Bytes.toBytes("YHOO")));
    float aaplVal = Bytes.toFloat(pricesDS.read(Bytes.toBytes("AAPL")));
    Assert.assertTrue(Math.abs(yhooTotal - yhooVal) < 0.0000001);
    Assert.assertTrue(Math.abs(aaplTotal - aaplVal) < 0.0000001);
  }

  private byte[] createEvent(Schema schema, String ticker, int count, float price) throws IOException {
    GenericRecord record = new GenericRecordBuilder(schema)
      .set("ticker", ticker)
      .set("num_traded", count)
      .set("price", price)
      .build();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);

    writer.write(record, encoder);
    encoder.flush();
    out.close();
    return out.toByteArray();
  }
}

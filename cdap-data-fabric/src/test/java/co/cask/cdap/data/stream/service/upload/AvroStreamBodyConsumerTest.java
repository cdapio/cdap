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

package co.cask.cdap.data.stream.service.upload;

import co.cask.http.BodyConsumer;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Unit test for {@link AvroStreamBodyConsumer}.
 */
public class AvroStreamBodyConsumerTest extends StreamBodyConsumerTestBase {

  @Override
  protected ContentInfo generateFile(final int recordCount) throws IOException {
    return new FileContentInfo(generateAvroFile(TMP_FOLDER.newFile(), recordCount)) {

      @Override
      public boolean verify(Map<String, String> headers,
                            InputSupplier<? extends InputStream> contentSupplier) throws IOException {
        // Deserialize and verify the records
        Decoder decoder = DecoderFactory.get().binaryDecoder(contentSupplier.getInput(), null);
        DatumReader<Record> reader = new ReflectDatumReader<Record>(Record.class);
        reader.setSchema(new Schema.Parser().parse(headers.get("schema")));
        for (int i = 0; i < recordCount; i++) {
          Record record = reader.read(null, decoder);
          if (i != record.id) {
            return false;
          }
          if (!("Record number " + i).equals(record.name)) {
            return false;
          }
        }
        return true;
      }
    };
  }

  @Override
  protected BodyConsumer createBodyConsumer(ContentWriterFactory contentWriterFactory) {
    return new AvroStreamBodyConsumer(contentWriterFactory);
  }

  private File generateAvroFile(File file, int recordCount) throws IOException {
    Schema schema = Schema.createRecord("Record", null, null, false);
    schema.setFields(ImmutableList.of(
      new Schema.Field("id", Schema.create(Schema.Type.INT), null, null),
      new Schema.Field("name", Schema.createUnion(ImmutableList.of(Schema.create(Schema.Type.NULL),
                                                                   Schema.create(Schema.Type.STRING))), null, null)
    ));

    DataFileWriter<Record> writer = new DataFileWriter<Record>(new ReflectDatumWriter<Record>(Record.class));
    try {
      writer.setCodec(CodecFactory.snappyCodec());
      writer.create(schema, file);

      for (int i = 0; i < recordCount; i++) {
        writer.append(new Record(i, "Record number " + i));
      }
    } finally {
      writer.close();
    }

    return file;
  }

  /**
   * A POJO for serialization.
   */
  public static final class Record {
    int id;
    String name;

    public Record() {
      // Needed by Avro
    }

    public Record(int id, String name) {
      this.id = id;
      this.name = name;
    }
  }
}

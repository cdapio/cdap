/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.format;

import com.google.common.base.Charsets;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * A {@link RecordFormat} that interprets the entire input as a single string.
 */
public class TextRecordFormat extends RecordFormat<ByteBuffer, StructuredRecord> {
  public static final String CHARSET = "charset";
  private Charset charset = Charsets.UTF_8;
  private String fieldName = "body";

  @Override
  public StructuredRecord read(ByteBuffer input) {
    String bodyAsStr = Bytes.toString(input, charset);
    return StructuredRecord.builder(schema).set(fieldName, bodyAsStr).build();
  }

  @Override
  protected Schema getDefaultSchema() {
    return Schema.recordOf("record", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  }

  @Override
  protected void validateSchema(Schema desiredSchema) throws UnsupportedTypeException {
    List<Schema.Field> fields = desiredSchema.getFields();
    if (fields.size() != 1 || fields.get(0).getSchema().getType() != Schema.Type.STRING) {
      throw new UnsupportedTypeException("Schema must be a record with a single string field.");
    }
  }

  @Override
  protected void configure(Map<String, String> settings) {
    String charsetStr = settings.get(CHARSET);
    if (charsetStr != null) {
      this.charset = Charset.forName(charsetStr);
    }
    this.fieldName = schema.getFields().get(0).getName();
  }
}

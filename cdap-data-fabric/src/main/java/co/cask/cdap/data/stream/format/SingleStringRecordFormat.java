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

package co.cask.cdap.data.stream.format;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.internal.io.Schema;
import co.cask.cdap.internal.io.StructuredRecord;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import com.google.common.base.Charsets;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * Stream record format that interprets the entire body as a single string.
 */
public class SingleStringRecordFormat extends StreamRecordFormat<StructuredRecord> {
  public static final String CHARSET = "charset";
  private static final Schema STRING_SCHEMA =
    Schema.recordOf("stringBody", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  private Charset charset = Charsets.UTF_8;

  @Override
  public StructuredRecord format(StreamEvent input) {
    String bodyAsStr = Bytes.toString(input.getBody(), charset);
    return StructuredRecord.builder(schema).set("body", bodyAsStr).build();
  }

  @Override
  protected Schema getDefaultSchema() {
    return STRING_SCHEMA;
  }

  @Override
  protected void validateDesiredSchema(Schema desiredSchema) throws UnsupportedTypeException {
    if (desiredSchema != null && !desiredSchema.equals(STRING_SCHEMA)) {
      throw new UnsupportedTypeException("Only the default schema is allowed for this format.");
    }
  }

  @Override
  protected void configure(Map<String, String> settings) {
    String charsetStr = settings.get(CHARSET);
    if (charsetStr != null) {
      this.charset = Charset.forName(charsetStr);
    }
  }
}

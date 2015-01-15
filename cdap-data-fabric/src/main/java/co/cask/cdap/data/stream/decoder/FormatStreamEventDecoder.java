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

package co.cask.cdap.data.stream.decoder;

import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventDecoder;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A {@link StreamEventDecoder} that decodes {@link StreamEvent} into {@link LongWritable} as key
 * and {@link StructuredRecord} as value for Mapper input. The key carries the event timestamp, while
 * the value is the stream event body formatted by some {@link RecordFormat}.
 */
public final class FormatStreamEventDecoder implements StreamEventDecoder<LongWritable, StructuredRecord> {
  private static final String HEADERS_FIELD = "headers";
  private final LongWritable key = new LongWritable();
  private final RecordFormat<ByteBuffer, StructuredRecord> bodyFormat;
  private final Schema eventSchema;
  private final Schema bodySchema;

  /**
   * Create a decoder for stream events that decodes the body of the stream using the given initialized format.
   *
   * @param bodyFormat Initialized format.
   */
  public FormatStreamEventDecoder(RecordFormat<ByteBuffer, StructuredRecord> bodyFormat) {
    this.bodyFormat = bodyFormat;
    this.bodySchema = bodyFormat.getSchema();
    List<Schema.Field> fields = Lists.newArrayList(
      Schema.Field.of(HEADERS_FIELD, Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))
    );
    fields.addAll(bodySchema.getFields());
    this.eventSchema = Schema.recordOf("streamEvent", fields);
  }

  @Override
  public DecodeResult<LongWritable, StructuredRecord> decode(StreamEvent event,
                                                             DecodeResult<LongWritable, StructuredRecord> result) {
    key.set(event.getTimestamp());
    StructuredRecord.Builder builder = StructuredRecord.builder(eventSchema)
      .set(HEADERS_FIELD, event.getHeaders());
    StructuredRecord body = bodyFormat.read(event.getBody());
    for (Schema.Field field : bodySchema.getFields()) {
      String fieldName = field.getName();
      builder.set(fieldName, body.get(fieldName));
    }
    StructuredRecord value = builder.build();
    return result.setKey(key).setValue(value);
  }
}

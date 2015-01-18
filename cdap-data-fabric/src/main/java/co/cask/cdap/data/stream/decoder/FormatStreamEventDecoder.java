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
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.GenericStreamEventData;
import co.cask.cdap.api.stream.StreamEventDecoder;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.io.LongWritable;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A {@link StreamEventDecoder} that decodes {@link StreamEvent} into {@link LongWritable} as key
 * and {@link GenericStreamEventData} as value for Mapper input. The key carries the event timestamp, while
 * the value is a {@link GenericStreamEventData>}, which contains the event headers and the event body
 * formatted by some {@link RecordFormat}.
 *
 * @param <T> Type of the stream body.
 */
public final class FormatStreamEventDecoder<T> implements StreamEventDecoder<LongWritable, GenericStreamEventData<T>> {
  private final LongWritable key = new LongWritable();
  private final RecordFormat<ByteBuffer, T> bodyFormat;

  /**
   * Create a decoder for stream events that decodes the body of the stream using the given initialized format.
   *
   * @param bodyFormat Initialized format.
   */
  public FormatStreamEventDecoder(RecordFormat<ByteBuffer, T> bodyFormat) {
    this.bodyFormat = bodyFormat;
  }

  @Override
  public DecodeResult<LongWritable, GenericStreamEventData<T>> decode(
    StreamEvent event, DecodeResult<LongWritable, GenericStreamEventData<T>> result) {
    key.set(event.getTimestamp());
    T body = bodyFormat.read(event.getBody());
    Map<String, String> headers = Objects.firstNonNull(event.getHeaders(), ImmutableMap.<String, String>of());
    return result.setKey(key).setValue(new GenericStreamEventData<T>(headers, body));
  }
}

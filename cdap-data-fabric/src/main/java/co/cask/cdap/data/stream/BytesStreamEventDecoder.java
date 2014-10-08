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

package co.cask.cdap.data.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventDecoder;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import java.nio.ByteBuffer;

/**
 * A {@link StreamEventDecoder} that decodes {@link StreamEvent} into {@link LongWritable} as key
 * and {@link BytesWritable} as value for Mapper input. The key carries the event timestamp, while
 * the value is the stream event body.
 */
public final class BytesStreamEventDecoder implements StreamEventDecoder<LongWritable, BytesWritable> {

  private final LongWritable key = new LongWritable();
  private BytesWritable value = new BytesWritable();

  @Override
  public DecodeResult<LongWritable, BytesWritable> decode(StreamEvent event,
                                                          DecodeResult<LongWritable, BytesWritable> result) {
    key.set(event.getTimestamp());
    value = getEventBody(event, value);
    return result.setKey(key).setValue(value);
  }

  private BytesWritable getEventBody(StreamEvent event, BytesWritable result) {
    ByteBuffer body = event.getBody();
    if (body.hasArray()) {
      // If the ByteBuffer is backed by an array, which is exactly the same as exposed by the ByteBuffer
      // simply use the back array. No copying is needed (because read from stream->mapper is synchronous).
      // Creating a new BytesWritable is more efficient as it doesn't need to do array copy,
      // which BytesWritable.set() does.
      if (body.array().length == body.remaining()) {
        return new BytesWritable(body.array());
      }
      // Otherwise, need to copy the byte[], done by the BytesWritable.set() method
      result.set(body.array(), body.arrayOffset() + body.position(), body.remaining());
      return result;
    }

    // Otherwise, need to copy to a new array
    byte[] copy = new byte[body.remaining()];
    body.mark();
    body.get(copy);
    body.reset();
    return new BytesWritable(copy);
  }
}

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
import com.google.common.base.Charsets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * A {@link StreamEventDecoder} that decodes {@link StreamEvent} into {@link LongWritable} as key and {@link Text} as
 * value for Mapper input. The key carries the event timestamp, while the text value is a UTF-8 decode of the
 * event body.
 */
public final class TextStreamEventDecoder implements StreamEventDecoder<LongWritable, Text> {

  private final LongWritable key = new LongWritable();
  private final Text value = new Text();

  @Override
  public DecodeResult<LongWritable, Text> decode(StreamEvent event, DecodeResult<LongWritable, Text> result) {
    key.set(event.getTimestamp());
    value.set(Charsets.UTF_8.decode(event.getBody()).toString());
    return result.setKey(key).setValue(value);
  }
}

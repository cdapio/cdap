/*
 * Copyright 2014 Cask, Inc.
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
 * An {@link StreamInputFormat} that decode {@link StreamEvent} into timestamp and body text.
 */
public class TextStreamInputFormat extends StreamInputFormat<LongWritable, Text> {

  @Override
  protected StreamEventDecoder<LongWritable, Text> createStreamEventDecoder() {
    return new StreamEventDecoder<LongWritable, Text>() {
      private final LongWritable key = new LongWritable();
      private final Text value = new Text();

      @Override
      public DecodeResult<LongWritable, Text> decode(StreamEvent event, DecodeResult<LongWritable, Text> result) {
        key.set(event.getTimestamp());
        value.set(Charsets.UTF_8.decode(event.getBody()).toString());
        return result.setKey(key).setValue(value);
      }
    };
  }
}

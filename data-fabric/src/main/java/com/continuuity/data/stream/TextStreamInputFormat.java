/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamEventDecoder;
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

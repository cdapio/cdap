package co.cask.cdap.templates.etl.batch.sources;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventDecoder;
import org.apache.hadoop.io.LongWritable;

/**
*
*/
public final class IdentityStreamEventDecoder implements StreamEventDecoder<LongWritable, StreamEvent> {

  private final LongWritable key = new LongWritable();

  @Override
  public DecodeResult<LongWritable, StreamEvent> decode(StreamEvent event,
                                                        DecodeResult<LongWritable, StreamEvent> result) {
    key.set(event.getTimestamp());
    return result.setKey(key).setValue(event);
  }
}

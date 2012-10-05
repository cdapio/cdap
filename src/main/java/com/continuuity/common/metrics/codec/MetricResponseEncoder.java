package com.continuuity.common.metrics.codec;

import com.continuuity.common.metrics.MetricResponse;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetricResponseEncoder for encoding the response from processing
 * MetricRequest.
 */
public class MetricResponseEncoder implements ProtocolEncoder {
  private static final Logger Log
      = LoggerFactory.getLogger(MetricResponseEncoder.class);
  private static final int capacity = (Integer.SIZE)/8;

  @Override
  public void encode(IoSession session, Object message, ProtocolEncoderOutput
    out) throws Exception {
    if(message instanceof MetricResponse) {
      MetricResponse response = (MetricResponse) message;

      // allocated the capacity.
      IoBuffer buffer = IoBuffer.allocate(capacity, false);

      // Write the response data to buffer.
      buffer.putInt(response.getStatus().getCode());
      buffer.flip();

      // Write it to session.
      out.write(buffer);
    }
  }

  @Override
  public void dispose(IoSession session) throws Exception {}

}

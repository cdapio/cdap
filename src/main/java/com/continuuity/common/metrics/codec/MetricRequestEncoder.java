package com.continuuity.common.metrics.codec;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 *
 */
public class MetricRequestEncoder implements ProtocolEncoder {
  private static final Logger Log
    = LoggerFactory.getLogger(MetricRequestEncoder.class);
  @Override
  public void encode(IoSession session, Object message, ProtocolEncoderOutput
    out) throws Exception {

    String request = (String) message;
    request = request + "\n";
    IoBuffer buffer = IoBuffer.allocate(request.length(), false);
    buffer.putString(request, Charset.defaultCharset().newEncoder());
    buffer.flip();
    out.write(buffer);
  }

  @Override
  public void dispose(IoSession session) throws Exception {
    // nothing to dispose.
  }
}

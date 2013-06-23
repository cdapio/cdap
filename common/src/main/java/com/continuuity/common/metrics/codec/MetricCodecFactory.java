package com.continuuity.common.metrics.codec;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;

/**
 * A {@link org.apache.mina.filter.codec.ProtocolCodecFactory} for writing
 * and reading the metric put response.
 */
public class MetricCodecFactory implements ProtocolCodecFactory {
  private ProtocolEncoder encoder;
  private ProtocolDecoder decoder;

  public MetricCodecFactory(boolean client) {
    if(client) {
      encoder = new MetricRequestEncoder();
      decoder = new MetricResponseDecoder();
    } else {
      encoder = new MetricResponseEncoder();
      decoder = new MetricRequestDecoder();
    }
  }

  @Override
  public ProtocolEncoder getEncoder(IoSession session) throws Exception {
    return encoder;
  }

  @Override
  public ProtocolDecoder getDecoder(IoSession session) throws Exception {
    return decoder;
  }
}

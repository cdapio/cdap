package com.continuuity.common.metrics.codec;

import com.continuuity.common.metrics.MetricResponse;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MetricResponseDecoder extends CumulativeProtocolDecoder {
  private static Logger Log
      = LoggerFactory.getLogger(MetricResponseDecoder.class);
  private static final int capacity = (Integer.SIZE)/8;

  @Override
  protected boolean doDecode(IoSession session, IoBuffer in,
                             ProtocolDecoderOutput out) throws Exception {
    if(in.remaining() >= capacity) {
      int code = in.getInt();
      MetricResponse.Status status;
      if(code == MetricResponse.Status.FAILED.getCode()) {
        status = MetricResponse.Status.FAILED;
      } else if(code == MetricResponse.Status.SUCCESS.getCode()) {
        status = MetricResponse.Status.SUCCESS;
      } else if(code == MetricResponse.Status.IGNORED.getCode()) {
        status = MetricResponse.Status.IGNORED;
      } else {
        status = MetricResponse.Status.INVALID;
      }
      MetricResponse response = new MetricResponse(status);
      out.write(response);
      return true;
    }
    in.clear();
    return false;
  }
}

package com.continuuity.examples.countandfilterwords;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

public class StreamSource extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(StreamSource.class);

  private OutputEmitter<String> output;

  public void process(StreamEvent event) {
    LOG.debug(this.getContext().getName() + ": Received event " + event);

    byte[] body = Bytes.toBytes(event.getBody());
    String line = Bytes.toString(body);

    LOG.debug(this.getContext().getName() + ": Emitting " + line);

    output.emit(line);
  }
}
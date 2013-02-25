package com.continuuity.examples.simplewriteandread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.examples.simplewriteandread.SimpleWriteAndReadFlow.KeyAndValue;

public class KeyValueSource extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(KeyValueSource.class);

  private OutputEmitter<KeyAndValue> output;

  public KeyValueSource() {
    super("source");
  }

  public void process(StreamEvent event) throws IllegalArgumentException {
    LOG.debug(this.getContext().getName() + ": Received event " + event);

    String text = Bytes.toString(Bytes.toBytes(event.getBody()));

    String [] fields = text.split("=");
    
    if (fields.length != 2) {
      throw new IllegalArgumentException("Input event must be in the form " +
          "'key=value', received '" + text + "'");
    }

    output.emit(new KeyAndValue(fields[0], fields[1]));
  }
}
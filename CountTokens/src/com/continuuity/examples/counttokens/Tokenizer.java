package com.continuuity.examples.counttokens;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

public class Tokenizer extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(Tokenizer.class);

  private OutputEmitter<String> output;

  public void process(String line) {
    LOG.debug("Received line: " + line);
    if (line == null || line.isEmpty()) {
      LOG.warn("Received empty line");
      return;
    }

    String [] tokens = tokenize(line);

    for (String token : tokens) {
      LOG.debug("Emitting token: " + token);
      output.emit(token);
    }
  }

  private String [] tokenize(String line) {
    String delimiters = "[ .-]";
    return line.split(delimiters);
  }
}

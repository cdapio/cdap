package com.continuuity.examples.countandfilterwords;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

public class Tokenizer extends AbstractFlowlet {

  @Output("tokens")
  private OutputEmitter<String> tokenOutput;

  @Output("counts")
  private OutputEmitter<String> countOutput;

  public void process(String line) {
    // Tokenize and emit each token to the filters
    for (String token : line.split("[ .-]")) {
      tokenOutput.emit(token);
      // Also emit to the 'all' counter for each token
      countOutput.emit("all");
    }
  }
}

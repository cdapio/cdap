package com.continuuity.example.countandfilterwords;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.HashMap;
import java.util.Map;

public class UpperCaseFilter extends AbstractFlowlet {
  private OutputEmitter<Map<String,String>> output;

  public UpperCaseFilter() {
    super("upper-filter");
  }

  public void process(Map<String, String> tupleIn) {
    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tupleIn);
    }
    String word = tupleIn.get("word");
    if (word == null) {
      return;
    }
    // filter words that are not upper-cased
    if (!Character.isUpperCase(word.charAt(0))) {
      return;
    }

    Map<String,String> tupleOut = new HashMap<String,String>();
    tupleOut.put("word", word);

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + tupleOut);
    }
    output.emit(tupleOut);
  }
}

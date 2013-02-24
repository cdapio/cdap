package com.continuuity.examples.counttokens;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Tokenizer extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(Tokenizer.class);

  @Output("splitOut")
  private OutputEmitter<Map<String,String>> output;

  public Tokenizer() {
    super("split");
  }

  public void process(Map<String, String> map) {
    final String[] fields = { "title", "text" };

    LOG.debug(this.getContext().getName() + ": Received tuple " + map);

    for (String field : fields) {
      tokenize(map.get(field), field);
    }
  }

  void tokenize(String str, String field) {
    if (str == null) {
      return;
    }
    final String delimiters = "[ .-]";
    String[] tokens = str.split(delimiters);

    for (String token : tokens) {
      Map<String,String> tuple = new HashMap<String,String>();
      tuple.put("field", field);
      tuple.put("word", token);

      LOG.debug(this.getContext().getName() + ": Emitting tuple " + output);

      output.emit(tuple);
    }
  }
}


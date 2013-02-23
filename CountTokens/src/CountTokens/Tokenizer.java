package CountTokens;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.HashMap;
import java.util.Map;

public class Tokenizer extends AbstractFlowlet {
  private OutputEmitter<Map<String,String>> output;
  public Tokenizer() {
    super("split");
  }

  public void process(Map<String, String> map) {
    final String[] fields = { "title", "text" };

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + map);
    }
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

      if (Common.debug) {
        System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);
      }
      output.emit(tuple);
    }
  }
}


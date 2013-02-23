package CountAndFilterWords;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Map;

public class Tokenizer extends AbstractFlowlet {
  private OutputEmitter<Map<String,String>> output;
  public Tokenizer() {
    super("Tokenizer");
  }
//  @Override
//  public void configure(FlowletSpecifier specifier) {
//    TupleSchema in = new TupleSchemaBuilder().
//        add("title", String.class).
//        add("text", String.class).
//        create();
//    specifier.getDefaultFlowletInput().setSchema(in);
//
//    TupleSchema out = new TupleSchemaBuilder().
//        add("field", String.class).
//        add("word", String.class).
//        create();
//    specifier.getDefaultFlowletOutput().setSchema(out);
//  }

  public void process(Map<String, String> map) throws CharacterCodingException {
    final String[] fields = { "title", "text" };

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + map);
    }
    for (String field : fields) {
      tokenize((String)map.get(field), field);
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

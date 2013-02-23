package CountAndFilterWords;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Map;

public class UpperCaseFilter extends AbstractFlowlet {
  private OutputEmitter<Map<String,String>> output;
  public UpperCaseFilter() {
    super("UpperCaseFilter");
  }

//  @Override
//  public void configure(FlowletSpecifier specifier) {
//    TupleSchema schema = new TupleSchemaBuilder().
//        add("field", String.class).
//        add("word", String.class).
//        create();
//    specifier.getDefaultFlowletInput().setSchema(schema);
//    specifier.getDefaultFlowletOutput().setSchema(schema);
//  }

  public void process(Map<String, String> map) throws CharacterCodingException {
    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received map " + map);
    }
    String word = map.get("word");
    if (word == null) {
      return;
    }
    // filter words that are not upper-cased
    if (!Character.isUpperCase(word.charAt(0))) {
      return;
    }

    Map<String,String> tuple = new HashMap<String,String>();
    tuple.put("word", word);

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);
    }
    output.emit(tuple);
  }
}

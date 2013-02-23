package CountRandom;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.nio.charset.CharacterCodingException;

public class NumberSplitter extends AbstractFlowlet {
  private OutputEmitter<Integer> output;

  public NumberSplitter() {
    super("NumberSplitter");
  }

//  @Override
//  public void configure(FlowletSpecifier specifier) {
//    TupleSchema inout = new TupleSchemaBuilder().
//      add("number", Integer.class).
//      create();
//    specifier.getDefaultFlowletInput().setSchema(inout);
//    specifier.getDefaultFlowletOutput().setSchema(inout);
//  }

  public void process(Integer number, InputContext context) throws CharacterCodingException {
    output.emit(new Integer(number % 10000));
    output.emit(new Integer(number % 1000));
    output.emit(new Integer(number % 100));
    output.emit(new Integer(number % 10));
  }
}

package CountOddAndEven;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.nio.charset.CharacterCodingException;

/**
 * Based on the whether number is odd or even it puts the number on
 * different streams.
 */
public class OddOrEven extends AbstractFlowlet {
  private OutputEmitter<Integer> evenOutput;
  private OutputEmitter<Integer> oddOutput;

  public OddOrEven() {
    super("OddOrEven");
  }
//  @Override
//  public void configure(final FlowletSpecifier specifier) {
//    TupleSchema schema = new TupleSchemaBuilder().add("number", Integer.class).create();
//    specifier.getDefaultFlowletInput().setSchema(schema);
//    specifier.getDefaultFlowletOutput().setSchema(schema);
//    specifier.addFlowletOutput("even").setSchema(schema);
//  }

  public void process(Integer number, InputContext context) throws CharacterCodingException {
    if(number.intValue() % 2 == 0) {
      evenOutput.emit(number);
    } else {
      oddOutput.emit(number);
    }
  }
}

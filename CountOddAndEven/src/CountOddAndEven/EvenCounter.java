package CountOddAndEven;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import java.nio.charset.CharacterCodingException;

/**
 * Counts number of even tuples.
 */
public class EvenCounter extends AbstractFlowlet {

  public EvenCounter() {
    super("EvenCounter");
  }
  private int count = 0;

//  @Override
//  public void configure(final FlowletSpecifier specifier) {
//    TupleSchema schema = new TupleSchemaBuilder().add("number", Integer.class).create();
//    specifier.getDefaultFlowletInput().setSchema(schema);
//  }

  public void process(Integer number) throws CharacterCodingException {
    count++;
  }


}

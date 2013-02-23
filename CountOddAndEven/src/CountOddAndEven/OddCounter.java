package CountOddAndEven;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import java.nio.charset.CharacterCodingException;

/**
 * Counts number of Odd tuples.
 */
public class OddCounter extends AbstractFlowlet {
  private int count = 0;

  public OddCounter() {
    super("OddCounter");
  }

//  @Override
//  public void configure(final FlowletSpecifier specifier) {
//    TupleSchema schema = new TupleSchemaBuilder().add("number", Integer.class).create();
//    specifier.getDefaultFlowletInput().setSchema(schema);
//  }

  public void process(Integer number) throws CharacterCodingException {
    count++;
  }

}

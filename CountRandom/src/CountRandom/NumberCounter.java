package CountRandom;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.InputContext;

import java.nio.charset.CharacterCodingException;

public class NumberCounter extends AbstractFlowlet {

  static final byte[] column = { 'c', 'o', 'u', 'n', 't' };

  Table counters;

  public NumberCounter() {
    super("NumberCounter");
  }

//  @Override
//  public void configure(FlowletSpecifier specifier) {
//    TupleSchema in = new TupleSchemaBuilder().
//      add("number", Integer.class).
//      create();
//    specifier.getDefaultFlowletInput().setSchema(in);
//  }
//  @Override
//  public void initialize() {
//    //super.initialize();
//    this.counters = this.getFlowletContext().getDataSet("counters");
//  }

  public void process(Integer number, InputContext context) throws CharacterCodingException {

    this.counters = this.getContext().getDataSet("counters");

    //getContext().getLogger().info("Processing integer " + number.intValue());

    try {
      counters.write(new Increment(number.toString().getBytes(), column, 1L));
    } catch (OperationException e) {
      throw new RuntimeException(e);
    }
  }

}

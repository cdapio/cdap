package CountAndFilterWords;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

import java.nio.charset.CharacterCodingException;
import java.util.Map;

public class CountByField extends AbstractFlowlet {

  public CountByField() {
    super("CountByField");
  }

  KeyValueTable counters;

//  @Override
//  public void configure(FlowletSpecifier configurator) {
//    TupleSchema in = new TupleSchemaBuilder().
//        add("field", String.class).
//        add("word", String.class).
//        create();
//
//    configurator.getDefaultFlowletInput().setSchema(in);
//  }


//  @Override
//  public void initialize() {
//    super.initialize();
//    this.counters = getFlowletContext().getDataSet(Common.counterTableName);
//  }

  public void process(Map<String, String> map) throws CharacterCodingException {

    this.counters = getContext().getDataSet(Common.counterTableName);

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + map);
    }

    String token = map.get("word");
    if (token == null) return;
    String field = map.get("field");
    if (field != null) token = field + ":" + token;

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Incrementing for " + token);
    }
    try {
      this.counters.increment(token.getBytes(), 1L);
    } catch (OperationException e) {
      throw new RuntimeException(e);
    }
  }
}

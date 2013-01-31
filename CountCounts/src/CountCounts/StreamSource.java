package CountCounts;

import com.continuuity.api.data.Increment;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

import java.util.HashMap;

public class StreamSource extends ComputeFlowlet {

  static String keyTotal = ":sourceTotal:";

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema out = new TupleSchemaBuilder().
        add("text", String.class).
        create();
    specifier.getDefaultFlowletOutput().setSchema(out);
    specifier.getDefaultFlowletInput().setSchema(TupleSchema.EVENT_SCHEMA);
  }

  CounterTable counters;

  @Override
  public void initialize() {
    super.initialize();
    this.counters = getFlowletContext().getDataSet(Common.tableName);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);
    }
    byte[] body = tuple.get("body");
    String text = body == null ? null :new String(body);

    Tuple output = new TupleBuilder().
        set("text", text).
        create();

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);
    }
    outputCollector.add(output);

    if (Common.count) {
      // emit an increment for the total number of documents ingested
      this.counters.increment(keyTotal);
    }
  }
}

package CountCounts;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

import java.util.HashMap;

public class StreamSource extends AbstractComputeFlowlet {

  @Override
  public void configure(StreamsConfigurator configurator) {
    TupleSchema out = new TupleSchemaBuilder().
        add("text", String.class).
        create();
    configurator.getDefaultTupleOutputStream().setSchema(out);
    configurator.getDefaultTupleInputStream().setSchema(TupleSchema.EVENT_SCHEMA);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);

    byte[] body = tuple.get("body");
    String text = body == null ? null :new String(body);

    Tuple output = new TupleBuilder().
        set("text", text).
        create();

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);

    outputCollector.emit(output);
  }
}

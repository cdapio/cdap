package CountTokens;

import com.continuuity.api.flow.flowlet.AbstractComputeFlowlet;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.StreamsConfigurator;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.TupleBuilderImpl;
import com.continuuity.flow.flowlet.internal.TupleSchemaBuilderImpl;

import java.util.HashMap;

public class StreamSource extends AbstractComputeFlowlet {

  @Override
  public void configure(StreamsConfigurator configurator) {
    TupleSchema out = new TupleSchemaBuilderImpl().
        add("title", String.class).
        add("text", String.class).
        create();
    configurator.getDefaultTupleOutputStream().setSchema(out);
    configurator.getDefaultTupleInputStream().setSchema(FlowStream.eventStreamSchema);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);

    HashMap<String, String> headers = tuple.get("headers");
    byte[] body = tuple.get("body");
    String title = headers.get("title");
    String text = body == null ? null :new String(body);

    Tuple output = new TupleBuilderImpl().
        set("title", title).
        set("text", text).
        create();

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);

    outputCollector.emit(output);
  }
}

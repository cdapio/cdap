package CountAndFilterWords;

import com.continuuity.api.data.*;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

public class WriterFlowlet extends AbstractComputeFlowlet {

  @Override
  public void configure(StreamsConfigurator configurator) {
    TupleSchema in = new TupleSchemaBuilder().
        add("title", String.class).
        add("text", String.class).
        create();
    configurator.getDefaultTupleInputStream().setSchema(in);

    TupleSchema out = new TupleSchemaBuilder().
        add("row", byte[].class).
        add("column", byte[].class).
        create();
    configurator.getDefaultTupleOutputStream().setSchema(out);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);

    // text should be in the form: row=X column=Y value=Z
    String text = tuple.get("text");
    String [] params = text.split(" ");
    if (params.length != 3) return;
    String [] rowParams = params[0].split("=");
    String [] columnParams = params[1].split("=");
    String [] valueParams = params[2].split("=");
    if (rowParams.length != 2 && !rowParams[0].equals("row")) return;
    if (columnParams.length != 2 && !columnParams[0].equals("column")) return;
    if (valueParams.length != 2 && !valueParams[0].equals("value")) return;
    byte [] row = rowParams[1].getBytes();
    byte [] column = columnParams[1].getBytes();
    byte [] value = valueParams[1].getBytes();
    Write write = new Write(row, column, value);
    outputCollector.emit(write);
    Tuple outputTuple = new TupleBuilder().
          set("row", row).
          set("column", column).
          create();
    outputCollector.emit(outputTuple);
  }
}

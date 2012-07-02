package SimpleWriteAndRead;

import com.continuuity.api.data.*;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

public class ReaderFlowlet extends AbstractComputeFlowlet {

  @Override
  public void configure(StreamsConfigurator configurator) {
    TupleSchema in = new TupleSchemaBuilder().
        add("row", byte[].class).
        add("column", byte[].class).
        create();
    configurator.getDefaultTupleInputStream().setSchema(in);

    TupleSchema out = new TupleSchemaBuilder().
        add("row", byte[].class).
        add("column", byte[].class).
        add("value", byte[].class).
        create();
    configurator.getDefaultTupleOutputStream().setSchema(out);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);

    // perform inline read of row+column
    byte [] row = tuple.get("row");
    byte [] column = tuple.get("column");
    Read read = new Read(row, column);
    ReadOperationExecutor executor =
      getFlowletLaunchContext().getReadExecutor();
    byte [] value = executor.execute(read).get(column);
    
    // output a tuple, adding the value
    Tuple outputTuple = new TupleBuilder().
        set("row", row).
        set("column", column).
        set("value", value).
        create();
    outputCollector.emit(outputTuple);
  }
}

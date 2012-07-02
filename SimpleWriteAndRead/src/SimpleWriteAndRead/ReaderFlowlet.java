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
  
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Read value (" +
          new String(value) + ") for row (" + new String(row) + ") column (" +
          new String(column) + ")");

  }
}

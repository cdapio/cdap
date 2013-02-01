package WordCountApp;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;

public class UniqueCounterFlowlet extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    specifier.getDefaultFlowletInput()
        .setSchema(UniqueCountTable.UNIQUE_COUNT_TABLE_TUPLE_SCHEMA);
  }
  
  private UniqueCountTable uniqueCountTable;

  @Override
  public void initialize() {
    try {
      this.uniqueCountTable = getFlowletContext().getDataSet("uniqueCount");
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(Tuple tuple, TupleContext context,
      OutputCollector collector) {
    try {
      this.uniqueCountTable.updateUniqueCount(tuple);
    } catch (OperationException e) {
      // Fail the process() call if we get an exception
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
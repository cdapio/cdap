package DependencyRandomNumber;

import com.continuuity.api.data.*;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

public class EvenOddCounter extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema in = new TupleSchemaBuilder().
        add("randomNumber", Long.class).
        create();
    specifier.getDefaultFlowletInput().setSchema(in);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext,
      OutputCollector outputCollector) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() +
          ": Received tuple " + tuple);

    // count the number of odd or even numbers
    long randomNumber = ((Long)tuple.get("randomNumber")).longValue();
    boolean isEven = (randomNumber % 2) == 0;
    
    // generate an increment operation
    Increment increment;
    if (isEven) increment = new Increment("even".getBytes(), 1);
    else increment = new Increment("odd".getBytes(), 1);
    
    // emit the increment operation
    outputCollector.add(increment);
  }
}

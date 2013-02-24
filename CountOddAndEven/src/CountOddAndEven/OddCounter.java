package CountOddAndEven;

import com.continuuity.api.annotation.Process;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Counts number of Odd tuples.
 */
public class OddCounter extends AbstractFlowlet {
  private int count = 0;

  public OddCounter() {
    super("OddCounter");
  }

  @Process("oddOut")
  public void process() {
    count++;
  }

}

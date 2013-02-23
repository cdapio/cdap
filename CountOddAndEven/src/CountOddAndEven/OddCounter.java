package CountOddAndEven;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Counts number of Odd tuples.
 */
public class OddCounter extends AbstractFlowlet {
  private int count = 0;

  public OddCounter() {
    super("OddCounter");
  }

  public void process(Integer number) {
    count++;
  }

}

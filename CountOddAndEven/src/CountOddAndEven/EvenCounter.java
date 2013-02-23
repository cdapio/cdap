package CountOddAndEven;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Counts number of even tuples.
 */
public class EvenCounter extends AbstractFlowlet {

  public EvenCounter() {
    super("EvenCounter");
  }
  private int count = 0;

  public void process(Integer number) {
    count++;
  }


}

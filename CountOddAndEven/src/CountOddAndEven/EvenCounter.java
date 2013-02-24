package CountOddAndEven;

import com.continuuity.api.annotation.Process;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Counts number of even tuples.
 */
public class EvenCounter extends AbstractFlowlet {

  private int count = 0;

  public EvenCounter() {
    super("EvenCounter");
  }

  @Process("evenOut")
  public void process() {
    count++;
  }
}

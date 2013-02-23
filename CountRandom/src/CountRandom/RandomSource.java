package CountRandom;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.GeneratorFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.Random;

public class RandomSource extends AbstractFlowlet  implements GeneratorFlowlet {
  Random random;
  long millis = 0;
  int direction = 1;

  private OutputEmitter<Integer> randomOutput;

  public RandomSource() {
    super("gen");
  }

  public void generate() throws InterruptedException {
    Integer randomNumber = new Integer(this.random.nextInt(10000));
    Thread.sleep(millis);
    millis += direction;
    if(millis > 100 || millis < 1) {
      direction = direction * -1;
    }
    randomOutput.emit(randomNumber);
  }
}

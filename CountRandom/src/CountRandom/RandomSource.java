package CountRandom;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.GeneratorFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.Random;

public class RandomSource extends AbstractGeneratorFlowlet {
  private OutputEmitter<Integer> randomOutput;

  Random random;
  long millis = 0;
  int direction = 1;

  public RandomSource() {
    super("RandomSource");
  }

  public void generate() throws Exception {
    Integer randomNumber = new Integer(this.random.nextInt(10000));
    try {
      Thread.sleep(millis);
      millis += direction;
      if(millis > 100 || millis < 1) {
        direction = direction * -1;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    randomOutput.emit(randomNumber);
  }
}

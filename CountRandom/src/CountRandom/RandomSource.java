package CountRandom;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.GeneratorFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.Random;

public class RandomSource extends AbstractGeneratorFlowlet {
  private OutputEmitter<Integer> randomOutput;

  private final Random random = new Random();
  private long millis = 0;
  private int direction = 1;

  public RandomSource() {
    super("gen");
  }

  public void generate() throws InterruptedException {
    Integer randomNumber = new Integer(this.random.nextInt(10000));
    Thread.sleep(millis);
    millis *= direction;
    if(millis > 100 || millis < 1) {
      direction = direction * -1;
    }
    randomOutput.emit(randomNumber);
  }
}

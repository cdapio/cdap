package com.continuuity.examples.countoddandeven;

import java.util.Random;

import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

/**
 * Random number generator.
 */
public class RandomNumberGenerator extends AbstractGeneratorFlowlet {

  Random random;
  long millis = 1;
  int direction = 1;

  private OutputEmitter<Integer> randomOutput;

  @Override
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

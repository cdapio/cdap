package com.continuuity.examples.countrandom;

import java.util.Random;

import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

public class RandomSource extends AbstractGeneratorFlowlet {
  private OutputEmitter<Integer> randomOutput;

  private final Random random = new Random();
  private long millis = 1;
  private int direction = 1;

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

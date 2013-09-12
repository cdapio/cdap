package com.continuuity.examples.resourcespammer;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.Random;

/**
 *
 */
public class DataGenerator extends AbstractGeneratorFlowlet {
  @Output("out")
  private OutputEmitter<Integer> randomOutput;

  private final Random random = new Random();

  public void generate() throws InterruptedException {
    Integer randomNumber = new Integer(this.random.nextInt());
    randomOutput.emit(randomNumber);
  }
}

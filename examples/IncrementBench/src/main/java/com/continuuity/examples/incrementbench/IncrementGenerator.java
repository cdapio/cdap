package com.continuuity.examples.incrementbench;

import com.continuuity.api.annotation.Tick;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class IncrementGenerator extends AbstractFlowlet {
  private static final int ROWKEY_LENGTH = 8;

  private OutputEmitter<byte[]> output;

  private Random random;

  public IncrementGenerator() {
    random = new Random();
  }

  @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
  public void generate() throws InterruptedException {
    output.emit(pickRow());
  }

  private byte[] pickRow() {
    byte[] nextRow = new byte[ROWKEY_LENGTH];
    random.nextBytes(nextRow);
    return nextRow;
  }
}

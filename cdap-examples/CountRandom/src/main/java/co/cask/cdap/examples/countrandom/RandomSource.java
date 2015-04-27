/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.examples.countrandom;

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Random Source Flowlet {@code RandomSource}.
 */
public class RandomSource extends AbstractFlowlet {
  private OutputEmitter<Integer> randomOutput;

  private final Random random = new Random();
  private long delay = 0;
  private boolean emit = true;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    String delayStr = context.getRuntimeArguments().get("delay");
    if (delayStr != null) {
      delay = Long.parseLong(delayStr);
    }
    String emitStr = context.getRuntimeArguments().get("emit");
    if (emitStr != null) {
      emit = Boolean.parseBoolean(emitStr);
    }
  }

  @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
  public void generate() throws InterruptedException {
    if (emit) {
      randomOutput.emit(random.nextInt(10000));
    }
    if (delay > 0) {
      TimeUnit.MILLISECONDS.sleep(delay);
    }
  }
}

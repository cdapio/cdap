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

package co.cask.cdap;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Count random app for testing.
 */
public class TestCountRandomApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("CountRandomApp");
    setDescription("Count Random Application");
    createDataset("counters", Table.class);
    addFlow(new CountRandom());
  }


  private static class CountRandom implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("CountRandom")
        .setDescription("CountRandom")
        .withFlowlets()
        .add("source", new RandomSource())
        .add("splitter", new NumberSplitter())
        .add("counter", new NumberCounter())
        .connect()
        .from("source").to("splitter")
        .from("splitter").to("counter")
        .build();
    }
  }

  private static class NumberCounter extends AbstractFlowlet {
    static final byte[] COLUMN = { 'c', 'o', 'u', 'n', 't' };

    @UseDataSet("counters")
    Table counters;

    public NumberCounter() {
      super("NumberCounter");
    }

    @ProcessInput
    public void process(Integer number) {
      counters.increment(number.toString().getBytes(), COLUMN, 1L);
    }

  }

  private static class NumberSplitter extends AbstractFlowlet {
    private OutputEmitter<Integer> output;

    public NumberSplitter() {
      super("NumberSplitter");
    }

    @ProcessInput
    public void process(Integer number)  {
      output.emit(new Integer(number % 10000));
      output.emit(new Integer(number % 1000));
      output.emit(new Integer(number % 100));
      output.emit(new Integer(number % 10));
    }
  }

  private static class RandomSource extends AbstractFlowlet {
    private OutputEmitter<Integer> randomOutput;

    private Random random;
    long millis = 0;
    int direction = 1;

    public RandomSource() {
      random = new Random();
    }

    @Tick(delay = 1L, unit = TimeUnit.NANOSECONDS)
    public void generate() throws Exception {
      Integer randomNumber = new Integer(this.random.nextInt(10000));
      try {
        Thread.sleep(millis);
        millis += direction;
        if (millis > 100 || millis < 1) {
          direction = direction * -1;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      randomOutput.emit(randomNumber);
    }
  }

}

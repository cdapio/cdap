package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Count random app for testing.
 */
public class TestCountRandomApp implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountRandomApp")
      .setDescription("Count Random Application")
      .noStream()
      .withDataSets().add(new Table("counters"))
      .withFlows().add(new CountRandom())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
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

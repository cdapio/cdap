package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.Random;

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
      .noBatch()
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
    static final byte[] column = { 'c', 'o', 'u', 'n', 't' };

    @UseDataSet("counters")
    Table counters;

    public NumberCounter() {
      super("NumberCounter");
    }

    public void process(Integer number) {
      try {
        counters.write(new Increment(number.toString().getBytes(), column, 1L));
      } catch (OperationException e) {
        throw new RuntimeException(e);
      }
    }

  }

  private static class NumberSplitter extends AbstractFlowlet {
    private OutputEmitter<Integer> output;

    public NumberSplitter() {
      super("NumberSplitter");
    }

    public void process(Integer number)  {
      output.emit(new Integer(number % 10000));
      output.emit(new Integer(number % 1000));
      output.emit(new Integer(number % 100));
      output.emit(new Integer(number % 10));
    }
  }

  private static class RandomSource extends AbstractGeneratorFlowlet {
    private OutputEmitter<Integer> randomOutput;

    private Random random;
    long millis = 0;
    int direction = 1;

    public RandomSource() {
      random = new Random();
    }

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

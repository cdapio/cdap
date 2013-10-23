package com.continuuity.runtime;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MultiConsumerTest {

  /**
   *
   */
  public static final class MultiApp implements Application {

    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
        .setName("MultiApp")
        .setDescription("MultiApp")
        .noStream()
        .withDataSets().add(new KeyValueTable("accumulated"))
        .withFlows().add(new MultiFlow())
        .noProcedure()
        .noMapReduce()
        .noWorkflow()
        .build();
    }
  }

  /**
   *
   */
  public static final class MultiFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("MultiFlow")
        .setDescription("MultiFlow")
        .withFlowlets()
          .add("gen", new Generator())
          .add("c1", new Consumer(), 2)
          .add("c2", new Consumer(), 2)
          .add("c3", new ConsumerStr(), 2)
        .connect()
          .from("gen").to("c1")
          .from("gen").to("c2")
          .from("gen").to("c3")
        .build();
    }
  }

  /**
   *
   */
  public static final class Generator extends AbstractFlowlet {

    private OutputEmitter<Integer> output;
    @Output("str")
    private OutputEmitter<String> outString;
    private int i;

    @Tick(delay = 1L, unit = TimeUnit.NANOSECONDS)
    public void generate() throws Exception {
      if (i < 100) {
        output.emit(i);
        outString.emit(Integer.toString(i));
        i++;
      }
    }
  }

  private static final byte[] KEY = new byte[] {'k', 'e', 'y'};

  /**
   *
   */
  public static final class Consumer extends AbstractFlowlet {
    @UseDataSet("accumulated")
    private KeyValueTable accumulated;

    @ProcessInput(maxRetries = Integer.MAX_VALUE)
    public void process(long l) {
      accumulated.increment(KEY, l);
    }
  }

  /**
   *
   */
  public static final class ConsumerStr extends AbstractFlowlet {
    @UseDataSet("accumulated")
    private KeyValueTable accumulated;

    @ProcessInput(value = "str", maxRetries = Integer.MAX_VALUE)
    public void process(String str) {
      accumulated.increment(KEY, Long.valueOf(str));
    }
  }

  @Test
  public void testMulti() throws Exception {
    // TODO: Fix this test case to really test with numGroups settings.
    final ApplicationWithPrograms app = TestHelper.deployApplicationWithManager(MultiApp.class);
    ProgramRunnerFactory runnerFactory = TestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    List<ProgramController> controllers = Lists.newArrayList();
    for (final Program program : app.getPrograms()) {
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
      controllers.add(runner.run(program, new ProgramOptions() {
        @Override
        public String getName() {
          return program.getName();
        }

        @Override
        public Arguments getArguments() {
          return new BasicArguments();
        }

        @Override
        public Arguments getUserArguments() {
          return new BasicArguments();
        }
      }));
    }

    TimeUnit.SECONDS.sleep(4);

    LocationFactory locationFactory = TestHelper.getInjector().getInstance(LocationFactory.class);
    DataSetAccessor dataSetAccessor = TestHelper.getInjector().getInstance(DataSetAccessor.class);

    DataSetInstantiator dataSetInstantiator =
      new DataSetInstantiator(new DataFabric2Impl(locationFactory, dataSetAccessor),
                              getClass().getClassLoader());
    dataSetInstantiator.setDataSets(new MultiApp().configure().getDataSets().values());

    final KeyValueTable accumulated = dataSetInstantiator.getDataSet("accumulated");
    TransactionExecutorFactory txExecutorFactory =
      TestHelper.getInjector().getInstance(TransactionExecutorFactory.class);

    txExecutorFactory.createExecutor(dataSetInstantiator.getTransactionAware())
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          byte[] value = accumulated.read(KEY);
          // Sum(1..100) * 3
          Assert.assertEquals(((1 + 99) * 99 / 2) * 3, Longs.fromByteArray(value));
        }
    });

    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
  }
}

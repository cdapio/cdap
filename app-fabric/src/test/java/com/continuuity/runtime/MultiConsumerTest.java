package com.continuuity.runtime;

import com.continuuity.api.Application;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Ignore
public class MultiConsumerTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Supplier<File> TEMP_FOLDER_SUPPLIER = new Supplier<File>() {

    @Override
    public File get() {
      try {
        return tmpFolder.newFolder();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  };

  /**
   *
   */
  public static final class MultiApp implements Application {

    @Override
    public com.continuuity.api.ApplicationSpecification configure() {
      return com.continuuity.api.ApplicationSpecification.Builder.with()
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
    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(MultiApp.class,
                                                                                         TEMP_FOLDER_SUPPLIER);
    ProgramRunnerFactory runnerFactory = AppFabricTestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    List<ProgramController> controllers = Lists.newArrayList();
    for (final Program program : app.getPrograms()) {
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
      controllers.add(runner.run(program, new SimpleProgramOptions(program)));
    }

    LocationFactory locationFactory = AppFabricTestHelper.getInjector().getInstance(LocationFactory.class);
    DataSetAccessor dataSetAccessor = AppFabricTestHelper.getInjector().getInstance(DataSetAccessor.class);
    DatasetFramework datasetFramework = AppFabricTestHelper.getInjector().getInstance(DatasetFramework.class);

    DataSetInstantiator dataSetInstantiator =
      new DataSetInstantiator(new DataFabric2Impl(locationFactory, dataSetAccessor),
                              datasetFramework, CConfiguration.create(),
                              getClass().getClassLoader());
    ApplicationSpecification spec = Specifications.from(new MultiApp().configure());
    dataSetInstantiator.setDataSets(spec.getDataSets().values(), spec.getDatasets().values());

    final KeyValueTable accumulated = dataSetInstantiator.getDataSet("accumulated");
    TransactionExecutorFactory txExecutorFactory =
      AppFabricTestHelper.getInjector().getInstance(TransactionExecutorFactory.class);

    // Try to get accumulated result and verify it. Expect result appear in max of 60 seconds.
    int trial = 0;
    while (trial < 60) {
      try {
        txExecutorFactory.createExecutor(dataSetInstantiator.getTransactionAware())
          .execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              byte[] value = accumulated.read(KEY);
              // Sum(1..100) * 3
              Assert.assertEquals(((1 + 99) * 99 / 2) * 3, Longs.fromByteArray(value));
            }
          });
        break;
      } catch (TransactionFailureException e) {
        // No-op
        trial++;
        TimeUnit.SECONDS.sleep(1);
      }
    }
    Assert.assertTrue(trial < 60);

    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
  }
}

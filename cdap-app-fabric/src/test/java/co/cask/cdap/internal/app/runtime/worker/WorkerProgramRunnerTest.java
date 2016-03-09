/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.worker;

import co.cask.cdap.AppWithWorker;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.internal.TempFolder;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.twill.common.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests running worker programs.
 */
@Category(SlowTests.class)
public class WorkerProgramRunnerTest {

  private static final TempFolder TEMP_FOLDER = new TempFolder();

  private static Injector injector;
  private static TransactionExecutorFactory txExecutorFactory;

  private static TransactionManager txService;
  private static DatasetFramework dsFramework;
  private static DynamicDatasetCache datasetCache;
  private static MetricStore metricStore;

  private static Collection<ProgramController> runningPrograms = new HashSet<>();

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

  @BeforeClass
  public static void beforeClass() {
    // we are only gonna do long-running transactions here. Set the tx timeout to a ridiculously low value.
    // that will test that the long-running transactions actually bypass that timeout.
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());
    conf.setInt(TxConstants.Manager.CFG_TX_TIMEOUT, 1);
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 2);
    injector = AppFabricTestHelper.getInjector(conf);
    txService = injector.getInstance(TransactionManager.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    dsFramework = injector.getInstance(DatasetFramework.class);
    datasetCache = new SingleThreadDatasetCache(
      new SystemDatasetInstantiator(dsFramework, WorkerProgramRunnerTest.class.getClassLoader(), null),
      injector.getInstance(TransactionSystemClient.class),
      NamespaceId.DEFAULT, DatasetDefinition.NO_ARGUMENTS, null, null);
    metricStore = injector.getInstance(MetricStore.class);

    txService.startAndWait();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    txService.stopAndWait();
  }

  @After
  public void after() throws Throwable {
    // stop all running programs
    for (ProgramController controller : runningPrograms) {
        stopProgram(controller);
    }
    // cleanup user data (only user datasets)
    for (DatasetSpecificationSummary spec : dsFramework.getInstances(DefaultId.NAMESPACE)) {
      dsFramework.deleteInstance(Id.DatasetInstance.from(DefaultId.NAMESPACE, spec.getName()));
    }
  }

  @Test
  public void testWorkerDatasetWithMetrics() throws Throwable {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(AppWithWorker.class, TEMP_FOLDER_SUPPLIER);

    ProgramController controller = startProgram(app, AppWithWorker.TableWriter.class);

    // validate worker wrote the "initialize" and "run" rows
    final TransactionExecutor executor = txExecutorFactory.createExecutor(datasetCache);

    // wait at most 5 seconds until the "RUN" row is set (indicates the worker has started running)
    Tasks.waitFor(AppWithWorker.RUN, new Callable<String>() {
      @Override
      public String call() throws Exception {
        return executor.execute(
          new Callable<String>() {
            @Override
            public String call() throws Exception {
              KeyValueTable kvTable = datasetCache.getDataset(AppWithWorker.DATASET);
              return Bytes.toString(kvTable.read(AppWithWorker.RUN));
            }
          });
      }
    }, 5, TimeUnit.SECONDS);

    stopProgram(controller);

    txExecutorFactory.createExecutor(datasetCache.getTransactionAwares()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          KeyValueTable kvTable = datasetCache.getDataset(AppWithWorker.DATASET);
          Assert.assertEquals(AppWithWorker.RUN, Bytes.toString(kvTable.read(AppWithWorker.RUN)));
          Assert.assertEquals(AppWithWorker.INITIALIZE, Bytes.toString(kvTable.read(AppWithWorker.INITIALIZE)));
          Assert.assertEquals(AppWithWorker.STOP, Bytes.toString(kvTable.read(AppWithWorker.STOP)));
        }
      });

    // validate that the table emitted metrics
    Tasks.waitFor(3L, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        Collection<MetricTimeSeries> metrics =
          metricStore.query(new MetricDataQuery(
            0,
            System.currentTimeMillis() / 1000L,
            60,
            "system." + Constants.Metrics.Name.Dataset.OP_COUNT,
            AggregationFunction.SUM,
            ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, DefaultId.NAMESPACE.getId(),
                            Constants.Metrics.Tag.APP, AppWithWorker.NAME,
                            Constants.Metrics.Tag.WORKER, AppWithWorker.WORKER,
                            Constants.Metrics.Tag.DATASET, AppWithWorker.DATASET),
            Collections.<String>emptyList()));
        if (metrics.isEmpty()) {
          return 0L;
        }
        Assert.assertEquals(1, metrics.size());
        MetricTimeSeries ts = metrics.iterator().next();
        Assert.assertEquals(1, ts.getTimeValues().size());
        return ts.getTimeValues().get(0).getValue();
      }
    }, 5L, TimeUnit.SECONDS, 50L, TimeUnit.MILLISECONDS);
  }

  private ProgramController startProgram(ApplicationWithPrograms app, Class<?> programClass)
    throws Throwable {
    final AtomicReference<Throwable> errorCause = new AtomicReference<>();
    final ProgramController controller = submit(app, programClass, RuntimeArguments.NO_ARGUMENTS);
    runningPrograms.add(controller);
    controller.addListener(new AbstractListener() {
      @Override
      public void error(Throwable cause) {
        errorCause.set(cause);
      }

      @Override
      public void killed() {
        errorCause.set(new RuntimeException("Killed"));
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Tasks.waitFor(ProgramController.State.ALIVE, new Callable<ProgramController.State>() {
      @Override
      public ProgramController.State call() throws Exception {
        Throwable t = errorCause.get();
        if (t != null) {
          Throwables.propagateIfInstanceOf(t, Exception.class);
          throw Throwables.propagate(t);
        }
        return controller.getState();
      }
    }, 30, TimeUnit.SECONDS);

    return controller;
  }

  private void stopProgram(ProgramController controller) throws Throwable {
    final AtomicReference<Throwable> errorCause = new AtomicReference<>();
    final CountDownLatch complete = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void error(Throwable cause) {
        complete.countDown();
        errorCause.set(cause);
      }

      @Override
      public void completed() {
        complete.countDown();
      }

      @Override
      public void killed() {
        complete.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    controller.stop();
    complete.await(30, TimeUnit.SECONDS);
    runningPrograms.remove(controller);
    Throwable t = errorCause.get();
    if (t != null) {
      throw t;
    }
  }

  private ProgramController submit(ApplicationWithPrograms app,
                                   Class<?> programClass,
                                   Map<String, String> userArgs) throws ClassNotFoundException {

    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    Program program = getProgram(app, programClass);
    Assert.assertNotNull(program);
    ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));

    BasicArguments systemArgs = new BasicArguments(ImmutableMap.of(ProgramOptionConstants.RUN_ID,
                                                                   RunIds.generate().getId()));
    return runner.run(program, new SimpleProgramOptions(program.getName(), systemArgs, new BasicArguments(userArgs)));
  }

  private Program getProgram(ApplicationWithPrograms app, Class<?> programClass) throws ClassNotFoundException {
    for (Program p : app.getPrograms()) {
      if (programClass.getCanonicalName().equals(p.getMainClass().getCanonicalName())) {
        return p;
      }
    }
    return null;
  }


}

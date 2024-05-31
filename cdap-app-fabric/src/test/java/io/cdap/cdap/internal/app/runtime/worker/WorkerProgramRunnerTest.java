/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.worker;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.cdap.cdap.AppWithMisbehavedDataset;
import io.cdap.cdap.AppWithWorker;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.table.Get;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DynamicDatasetCache;
import io.cdap.cdap.data2.dataset2.SingleThreadDatasetCache;
import io.cdap.cdap.data2.transaction.TransactionExecutorFactory;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.DefaultId;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.SlowTests;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.twill.common.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Tests running worker programs.
 */
@Category(SlowTests.class)
public class WorkerProgramRunnerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static TransactionExecutorFactory txExecutorFactory;

  private static TransactionManager txService;
  private static DatasetFramework dsFramework;
  private static DynamicDatasetCache datasetCache;
  private static MetricStore metricStore;

  private static Collection<ProgramController> runningPrograms = new HashSet<>();

  private static final Supplier<File> TEMP_FOLDER_SUPPLIER = new Supplier<File>() {
    @Override
    public File get() {
      try {
        return TEMP_FOLDER.newFolder();
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
    conf.setInt(TxConstants.Manager.CFG_TX_TIMEOUT, 1);
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 2);

    Injector injector = AppFabricTestHelper.getInjector(conf);
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
  public static void afterClass() {
    txService.stopAndWait();
    AppFabricTestHelper.shutdown();
  }

  @After
  public void after() throws Throwable {
    // stop all running programs
    for (ProgramController controller : runningPrograms) {
        stopProgram(controller);
    }
    // cleanup user data (only user datasets)
    for (DatasetSpecificationSummary spec : dsFramework.getInstances(DefaultId.NAMESPACE)) {
      dsFramework.deleteInstance(DefaultId.NAMESPACE.dataset(spec.getName()));
    }
  }

  @Test
  public void testWorkerWithMisbehavedDataset() throws Throwable {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(AppWithMisbehavedDataset.class, TEMP_FOLDER_SUPPLIER);
    final ProgramController controller = startProgram(app, AppWithMisbehavedDataset.TableWriter.class);
    Tasks.waitFor(ProgramController.State.COMPLETED, new Callable<ProgramController.State>() {
      @Override
      public ProgramController.State call() throws Exception {
        return controller.getState();
      }
    }, 30, TimeUnit.SECONDS);

    // validate worker was able to execute its second transaction
    final TransactionExecutor executor = txExecutorFactory.createExecutor(datasetCache);
    executor.execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Table table = datasetCache.getDataset(AppWithMisbehavedDataset.TABLE);
          Row result = table.get(new Get(AppWithMisbehavedDataset.ROW, AppWithMisbehavedDataset.COLUMN));
          Assert.assertEquals(AppWithMisbehavedDataset.VALUE, result.getString(AppWithMisbehavedDataset.COLUMN));
        }
      });
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
            Integer.MAX_VALUE,
            "system." + Constants.Metrics.Name.Dataset.OP_COUNT,
            AggregationFunction.SUM,
            ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, DefaultId.NAMESPACE.getEntityName(),
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
    final ProgramController controller = AppFabricTestHelper.submit(app, programClass.getName(),
                                                                    new BasicArguments(), TEMP_FOLDER_SUPPLIER);
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
}

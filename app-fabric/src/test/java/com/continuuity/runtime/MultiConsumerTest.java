/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.runtime;

import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.runtime.app.MultiApp;
import com.continuuity.tephra.TransactionExecutor;
import com.continuuity.tephra.TransactionExecutorFactory;
import com.continuuity.tephra.TransactionFailureException;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
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
              byte[] value = accumulated.read(MultiApp.KEY);
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

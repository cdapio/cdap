/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.runtime;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.runtime.app.MultiApp;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
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
    List<ProgramController> controllers = Lists.newArrayList();
    for (ProgramDescriptor programDescriptor : app.getPrograms()) {
      controllers.add(AppFabricTestHelper.submit(app, programDescriptor.getSpecification().getClassName(),
                                                 new BasicArguments(), TEMP_FOLDER_SUPPLIER)
      );
    }

    DatasetFramework datasetFramework = AppFabricTestHelper.getInjector().getInstance(DatasetFramework.class);

    DynamicDatasetCache datasetCache = new SingleThreadDatasetCache(
      new SystemDatasetInstantiator(datasetFramework, getClass().getClassLoader(), null),
      AppFabricTestHelper.getInjector().getInstance(TransactionSystemClient.class),
      NamespaceId.DEFAULT, DatasetDefinition.NO_ARGUMENTS, null, null);

    final KeyValueTable accumulated = datasetCache.getDataset("accumulated");
    TransactionExecutorFactory txExecutorFactory =
      AppFabricTestHelper.getInjector().getInstance(TransactionExecutorFactory.class);

    // Try to get accumulated result and verify it. Expect result appear in max of 60 seconds.
    int trial = 0;
    while (trial < 60) {
      try {
        Transactions.createTransactionExecutor(txExecutorFactory, accumulated)
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

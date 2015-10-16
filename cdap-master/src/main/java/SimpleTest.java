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

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.runtime.DiscoveryModules;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SimpleTest {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleTest.class);

  public static void main(String[] args) throws Exception {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryModules().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
        }
      }
    );

    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    final DatasetFramework dsFramework = injector.getInstance(DatasetFramework.class);
    final Id.DatasetInstance id = Id.DatasetInstance.from("custom", "history");
    int count = Integer.parseInt(args[0]);
    LOG.error("Starting {} threads", count);
    final CyclicBarrier barrier = new CyclicBarrier(count + 1);
    ExecutorService executor = Executors.newFixedThreadPool(count);
    for (int i = 0; i < count; i++) {
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            barrier.await();
            long start = System.nanoTime();

            Dataset dataset = dsFramework.getDataset(
              id, null, null, new ConstantClassLoaderProvider(null),
              ImmutableList.of(Id.Program.from("foo", "noapp", ProgramType.SERVICE, "noprogram")));
            if (dataset == null) {
              LOG.error("Not exists");
            }

            long elapse = System.nanoTime() - start;
            LOG.error("Call time: " + TimeUnit.NANOSECONDS.toMillis(elapse));
          } catch (Exception e) {
            LOG.error("Got exception: ", e);
          }
        }
      });
    }

    long start = System.nanoTime();
    barrier.await();
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.DAYS);
    long elapse = System.nanoTime() - start;
    LOG.error("Time: " + TimeUnit.NANOSECONDS.toMillis(elapse));

    zkClient.stopAndWait();
  }
}

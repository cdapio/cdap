/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.metrics.process;

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.metrics.MessagingMetricsTestBase;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Testing the basic properties of the {@link MessagingMetricsProcessorService}.
 */
public class MessagingMetricsProcessorServiceTest extends MessagingMetricsTestBase {
  // Intentionally set meta and fetcher persist threshold to small values, so that MessagingMetricsProcessorService
  // internally can exhaust message iterator and complete the loop.
  private static final int MESSAGING_META_PERSIST_THRESHOLD = 3;
  private static final int MESSAGING_FETCHER_PERSIST_THRESHOLD = 2;

  @Test
  public void testMetricsProcessor() {
    Set<Integer> partitions = new HashSet<>();
    for (int i = 0; i < partitionSize; i++) {
      partitions.add(i);
    }
    MessagingMetricsProcessorService metricsProcessorService =
      new MessagingMetricsProcessorService(injector.getInstance(MetricDatasetFactory.class), topicPrefix,
                                           partitions, messagingService, injector.getInstance(SchemaGenerator.class),
                                           injector.getInstance(DatumReaderFactory.class),
                                           injector.getInstance(MetricStore.class),
                                           MESSAGING_META_PERSIST_THRESHOLD,
                                           MESSAGING_FETCHER_PERSIST_THRESHOLD);
  }

  @Override
  protected List<Module> getAdditionalModules() {
    List<Module> list = new ArrayList<>();
    list.add(new DataSetsModules().getStandaloneModules());
    list.add(new IOModule());
    list.add(Modules.override(
      new NonCustomLocationUnitTestModule().getModule(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new ExploreClientModule(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule()
    ).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
      }
    }));
    return list;
  }
}

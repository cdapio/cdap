/*
 * Copyright © 2019 Cask Data, Inc.
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

import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.MetricsSystemClient;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.InMemoryDiscoveryModule;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.guice.MessagingServerRuntimeModule;
import co.cask.cdap.metrics.collect.LocalMetricsCollectionService;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.LocalMetricsDatasetFactory;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link MetricsAdminSubscriberService}.
 */
public class MetricsAdminSubscriberServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static MetricsCollectionService metricsCollectionService;
  private static MessagingService messagingService;
  private static MetricsQueryService metricsQueryService;
  private static Injector injector;

  @BeforeClass
  public static void init() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    // Shorten delay to speed up test
    cConf.setLong(Constants.Metrics.ADMIN_POLL_DELAY_MILLIS, 100L);

    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new IOModule(),
      new InMemoryDiscoveryModule(),
      new MessagingServerRuntimeModule().getStandaloneModules(),
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      // Instead of using the standard MetricsClientRuntimeModule, we need to define custom bindings here
      // This is because we want to test the MetricsAdminSubscriberService, which only being used in
      // distributed mode. It requires bindings that are too cumbersome to construct them one by one.
      new PrivateModule() {
        @Override
        protected void configure() {
          install(new MetricsHandlerModule());
          expose(MetricsQueryService.class);

          bind(MetricDatasetFactory.class).to(LocalMetricsDatasetFactory.class).in(Scopes.SINGLETON);
          bind(MetricStore.class).to(DefaultMetricStore.class);

          bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
          expose(MetricsCollectionService.class);

          // Bind the RemoteMetricsSystemClient for testing.
          bind(MetricsSystemClient.class).to(DirectMetricsSystemClient.class);
          expose(MetricsSystemClient.class);

          // Bind the admin subscriber
          bind(MetricsAdminSubscriberService.class).in(Scopes.SINGLETON);
          expose(MetricsAdminSubscriberService.class);
        }
      }
    );

    messagingService = injector.getInstance(MessagingService.class);
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsQueryService = injector.getInstance(MetricsQueryService.class);

    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    metricsCollectionService.startAndWait();
    metricsQueryService.startAndWait();
  }

  @AfterClass
  public static void finish() {
    metricsQueryService.stopAndWait();
    metricsCollectionService.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void test() throws Exception {
    MetricsAdminSubscriberService adminService = injector.getInstance(MetricsAdminSubscriberService.class);
    adminService.startAndWait();

    // publish a metrics
    MetricsContext metricsContext = metricsCollectionService.getContext(
      Collections.singletonMap(Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace()));
    metricsContext.increment("test.increment", 10L);
    metricsContext.gauge("test.gauge", 20L);

    MetricsSystemClient systemClient = injector.getInstance(RemoteMetricsSystemClient.class);

    // Search for metrics names
    Tasks.waitFor(true, () -> {
      Set<String> names = new HashSet<>(systemClient.search(metricsContext.getTags()));
      return names.contains("system.test.increment") && names.contains("system.test.gauge");
    }, 10, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    // Query for metrics values
    Tasks.waitFor(true, () -> {
      Collection<MetricTimeSeries> values = systemClient.query(metricsContext.getTags(),
                                                               Arrays.asList("system.test.increment",
                                                                             "system.test.gauge"));
      // Find and match the values for the increment and gauge
      boolean incMatched = values.stream()
        .filter(timeSeries -> timeSeries.getMetricName().equals("system.test.increment"))
        .flatMap(timeSeries -> timeSeries.getTimeValues().stream())
        .findFirst()
        .filter(timeValue -> timeValue.getValue() == 10L)
        .isPresent();

      boolean gaugeMatched = values.stream()
        .filter(timeSeries -> timeSeries.getMetricName().equals("system.test.gauge"))
        .flatMap(timeSeries -> timeSeries.getTimeValues().stream())
        .findFirst()
        .filter(timeValue -> timeValue.getValue() == 20L)
        .isPresent();

      return incMatched && gaugeMatched;
    }, 10, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    // Emit more metrics
    metricsContext.increment("test.increment", 40L);
    metricsContext.gauge("test.gauge", 40L);

    // Query for metrics values. Should see the latest aggregates
    Tasks.waitFor(true, () -> {
      Collection<MetricTimeSeries> values = systemClient.query(metricsContext.getTags(),
                                                               Arrays.asList("system.test.increment",
                                                                             "system.test.gauge"));
      // Find and match the values for the increment and gauge
      boolean incMatched = values.stream()
        .filter(timeSeries -> timeSeries.getMetricName().equals("system.test.increment"))
        .flatMap(timeSeries -> timeSeries.getTimeValues().stream())
        .findFirst()
        .filter(timeValue -> timeValue.getValue() == 50L)
        .isPresent();

      boolean gaugeMatched = values.stream()
        .filter(timeSeries -> timeSeries.getMetricName().equals("system.test.gauge"))
        .flatMap(timeSeries -> timeSeries.getTimeValues().stream())
        .findFirst()
        .filter(timeValue -> timeValue.getValue() == 40L)
        .isPresent();

      return incMatched && gaugeMatched;
    }, 10, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);


    // Delete the increment metrics
    systemClient.delete(new MetricDeleteQuery(0, Integer.MAX_VALUE,
                                              Collections.emptySet(),
                                              metricsContext.getTags(),
                                              new ArrayList<>(metricsContext.getTags().keySet())));

    Tasks.waitFor(true, () -> {
      Collection<MetricTimeSeries> values = systemClient.query(metricsContext.getTags(),
                                                               Arrays.asList("system.test.increment",
                                                                             "system.test.gauge"));
      // increment should be missing
      boolean foundInc = values.stream()
        .anyMatch(timeSeries -> timeSeries.getMetricName().equals("system.test.increment"));

      // Find and match the values for gauge
      boolean foundGauge = values.stream()
        .anyMatch(timeSeries -> timeSeries.getMetricName().equals("system.test.gauge"));

      return !foundInc && !foundGauge;
    }, 1000, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    adminService.stopAndWait();
  }
}

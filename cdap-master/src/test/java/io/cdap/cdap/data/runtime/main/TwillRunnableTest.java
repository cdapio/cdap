/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.data.runtime.main;

import com.google.inject.Injector;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.test.MockTwillContext;
import io.cdap.cdap.data.tools.HBaseTableExporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for master services.
 */
public class TwillRunnableTest {
  @Test
  public void testExploreServiceTwillRunnableInjector() {
    ExploreServiceTwillRunnable.createInjector(CConfiguration.create(), new Configuration(), "");
  }

  @Test
  public void testDatasetOpExecutorTwillRunnableInjector() {
    Injector injector = DatasetOpExecutorServerTwillRunnable.createInjector(CConfiguration.create(),
                                                                            HBaseConfiguration.create(), "");
    Store store = injector.getInstance(Store.class);
    Assert.assertNotNull(store);
    NamespaceQueryAdmin namespaceQueryAdmin = injector.getInstance(NamespaceQueryAdmin.class);
    Assert.assertNotNull(namespaceQueryAdmin);
  }

  @Test
  public void testHBaseTableExporterInjector() {
    HBaseTableExporter.createInjector(CConfiguration.create(), new Configuration());
  }

  @Test
  public void testMessagingServiceTwillRunnableInjector() {
    MessagingServiceTwillRunnable.createInjector(CConfiguration.create(), new Configuration());
  }

  @Test
  public void testMetricsTwillRunnableInjector() {
    MetricsTwillRunnable.createGuiceInjector(CConfiguration.create(), HBaseConfiguration.create(), "");
  }

  @Test
  public void testMetricsProcessorTwillRunnableInjector() {
    MetricsProcessorTwillRunnable.createGuiceInjector(CConfiguration.create(), new Configuration(), "",
                                                      new MockTwillContext());
  }

  @Test
  public void testLogSaverTwillRunnableInjector() {
    LogSaverTwillRunnable.createGuiceInjector(CConfiguration.create(), new Configuration(), new MockTwillContext());
  }
}

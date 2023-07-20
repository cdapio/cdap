/*
 * Copyright © 2022-2023 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.cdap.cdap.metrics.jmx.JmxMetricsCollector;
import io.cdap.cdap.metrics.jmx.JmxMetricsCollectorFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SystemMetricsExporterServiceMain}.
 */
public class SystemMetricsExporterServiceMainTest extends MasterServiceMainTestBase {

  @Test
  public void testSystemMetricsExporterService() {
    Injector injector = getServiceMainInstance(SystemMetricsExporterServiceMain.class)
        .getInjector();
    JmxMetricsCollectorFactory factory = injector.getInstance(JmxMetricsCollectorFactory.class);
    Map<String, String> metricTags = ImmutableMap.of("key1", "value1", "key2", "value2");
    JmxMetricsCollector metricsCollector = factory.create(metricTags);
    // JMX server isn't running, but that shouldn't raise exceptions, errors will be logged.
    metricsCollector.startAndWait();
    Assert.assertTrue(metricsCollector.isRunning());
    metricsCollector.stopAndWait();
    Assert.assertFalse(metricsCollector.isRunning());
  }

  @Test
  public void testFilterByKeyPrefix() {
    Map<String, String> values;
    Map<String, String> want;
    String prefix = "a.b.";

    // test with empty map
    values = new HashMap<>();
    want = new HashMap<>();
    Assert.assertEquals(want, SystemMetricsExporterServiceMain.filterByKeyPrefix(values, prefix));

    values = ImmutableMap.of("a.b.key1", "val1", "abc.key2", "val2");
    want = ImmutableMap.of("key1", "val1");
    Assert.assertEquals(want, SystemMetricsExporterServiceMain.filterByKeyPrefix(values, prefix));

    // test with empty prefix
    prefix = "";
    values = ImmutableMap.of("a.b.key1", "val1", "abc.key2", "val2");
    want = ImmutableMap.of("a.b.key1", "val1", "abc.key2", "val2");
    Assert.assertEquals(want, SystemMetricsExporterServiceMain.filterByKeyPrefix(values, prefix));
  }
}

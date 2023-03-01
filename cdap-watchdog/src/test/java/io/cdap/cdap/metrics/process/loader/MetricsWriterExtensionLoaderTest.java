/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process.loader;

import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link MetricsWriterExtensionLoader}
 */
public class MetricsWriterExtensionLoaderTest {

  @Test
  public void testEnabledMetricsWriterFilter() {
    //Test default value
    String mockWriterName = "mock-writer";
    CConfiguration cConf = CConfiguration.create();
    MetricsWriter mockWriter = new MockMetricsWriter(mockWriterName);
    MetricsWriterExtensionLoader writerExtensionLoader1 = new MetricsWriterExtensionLoader(cConf);
    Set<String> test1 = writerExtensionLoader1.getSupportedTypesForProvider(mockWriter);
    Assert.assertTrue(test1.isEmpty());

    //Test with writer ID enabled
    cConf.setStrings(Constants.Metrics.METRICS_WRITER_EXTENSIONS_ENABLED_LIST, mockWriterName);
    MetricsWriterExtensionLoader writerExtensionLoader2 = new MetricsWriterExtensionLoader(cConf);
    Set<String> test2 = writerExtensionLoader2.getSupportedTypesForProvider(mockWriter);
    Assert.assertTrue(test2.contains(mockWriterName));

    //Test comma separated writer IDs
    cConf.setStrings(Constants.Metrics.METRICS_WRITER_EXTENSIONS_ENABLED_LIST, mockWriterName + ",writer2");
    MetricsWriterExtensionLoader writerExtensionLoader3 = new MetricsWriterExtensionLoader(cConf);
    Set<String> test3 = writerExtensionLoader3.getSupportedTypesForProvider(mockWriter);
    Assert.assertTrue(test3.contains(mockWriterName));
    Assert.assertFalse(test3.contains("writer2"));

    cConf.setStrings(Constants.Metrics.METRICS_WRITER_EXTENSIONS_ENABLED_LIST, mockWriterName + ",writer2");
    MetricsWriterExtensionLoader writerExtensionLoader4 = new MetricsWriterExtensionLoader(cConf);
    Set<String> test4 = writerExtensionLoader4.getSupportedTypesForProvider(new MockMetricsWriter("writer2"));
    Assert.assertFalse(test4.contains(mockWriterName));
    Assert.assertTrue(test4.contains("writer2"));
  }
}

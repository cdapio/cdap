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

package io.cdap.cdap.metrics.publisher;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.api.metrics.MetricsWriterContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit test for {@link MetricsWritersMetricsPublisher}.
 */
public class MetricsWritersMetricsPublisherTest {
  @Mock
  private MetricsWriterProvider provider;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void beforeEach() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testPublishFlow() throws Exception {
    int numWriters = 5;
    Map<String, MetricsWriter> writers = getMockWriters(numWriters);
    when(provider.loadMetricsWriters()).thenReturn(writers);
    CConfiguration cConf = CConfiguration.create();
    MetricsWritersMetricsPublisher publisher = new MetricsWritersMetricsPublisher(provider, cConf);
    publisher.initialize();

    // Ensure all writers are initialized
    for (Map.Entry<String, MetricsWriter> entry : writers.entrySet()) {
      MetricsWriter writer = entry.getValue();
      verify(writer, times(1)).initialize(any());
      verify(writer, times(0)).write(any());
    }

    Collection<MetricValues> metricValues = getMockMetricValuesArray(5);
    publisher.publish(metricValues);

    // Ensure all writers receive the same metrics
    for (Map.Entry<String, MetricsWriter> entry : writers.entrySet()) {
      MetricsWriter writer = entry.getValue();
      verify(writer, times(1)).write(metricValues);
    }

    // Ensure if one writer throws exception on write(), rest still get metrics
    MetricsWriter faultyWriter = writers.get(getWriterName(2));
    doThrow(RuntimeException.class).when(faultyWriter).write(metricValues);
    publisher.publish(metricValues);
    for (Map.Entry<String, MetricsWriter> entry : writers.entrySet()) {
      MetricsWriter writer = entry.getValue();
      verify(writer, times(2)).write(metricValues);
    }

    publisher.close();

    // Ensure all writers are closed
    for (Map.Entry<String, MetricsWriter> entry : writers.entrySet()) {
      MetricsWriter writer = entry.getValue();
      verify(writer, times(1)).close();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testPublishingWithoutInitialization() throws Exception {
    int numWriters = 1;
    Map<String, MetricsWriter> writers = getMockWriters(numWriters);
    when(provider.loadMetricsWriters()).thenReturn(writers);
    CConfiguration cConf = CConfiguration.create();
    MetricsWritersMetricsPublisher publisher = new MetricsWritersMetricsPublisher(provider, cConf);
    publisher.publish(getMockMetricArray(2), new TreeMap<>());
  }

  @Test
  public void testWriterInititlaization() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    String filePath = tmpFolder.newFolder().getAbsolutePath();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, filePath);
    MetricsWritersMetricsPublisher publisher = new MetricsWritersMetricsPublisher(provider, cConf);

    //create test metricWriter that throws exception
    MetricsWriter mockWriter = new MetricsWriter() {

      @Override
      public void close() throws IOException {

      }

      @Override
      public void write(Collection<MetricValues> metricValues) {

      }

      @Override
      public void initialize(MetricsWriterContext metricsWriterContext) {
        throw new IllegalArgumentException("Not configured correctly");
      }

      @Override
      public String getID() {
        return "test_writer_exception";
      }
    };
    Map<String, MetricsWriter> writers = new TreeMap<>();
    writers.put(mockWriter.getID(), mockWriter);
    when(provider.loadMetricsWriters()).thenReturn(writers);
    try {
      publisher.initialize();
      Assert.fail("Expected exception, but method succeeded");
    } catch (Exception e) {
      //expected
    }

    //create the init file
    File base = new File(filePath, "metricswriters");
    base.mkdirs();
    new File(base, mockWriter.getID()).createNewFile();
    try {
      publisher.initialize();
    } catch (Exception e) {
      Assert.fail("Not expecting exception, but received one");
    }
  }

  private Map<String, MetricsWriter> getMockWriters(int count) {
    Map<String, MetricsWriter> writers = new TreeMap<>();
    for (int i = 0; i < count; ++i) {
      MetricsWriter writer = mock(MetricsWriter.class);
      String name = getWriterName(i);
      when(writer.getID()).thenReturn(name);
      writers.put(name, writer);
    }
    return writers;
  }

  private Collection<MetricValues> getMockMetricValuesArray(int num) {
    MetricValue metric = new MetricValue("mock.metric", MetricType.COUNTER, 1);
    Collection<MetricValues> metricValues = new ArrayList<>();
    for (int mockTime = 0; mockTime < num; ++mockTime) {
      metricValues.add(
        new MetricValues(new TreeMap<>(), metric.getName(), mockTime, metric.getValue(), metric.getType()));
    }
    return metricValues;
  }

  private Collection<MetricValue> getMockMetricArray(int num) {
    MetricValue metric = new MetricValue("mock.metric", MetricType.COUNTER, 1);
    return new ArrayList<>(Collections.nCopies(num, metric));
  }

  private String getWriterName(int index) {
    return String.format("Writer #%d", index);
  }
}

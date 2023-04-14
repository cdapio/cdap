/*
 * Copyright © 2020-2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.io.BinaryDecoder;
import io.cdap.cdap.common.io.BinaryEncoder;
import io.cdap.cdap.common.io.DatumReader;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.io.Encoder;
import io.cdap.cdap.common.io.StringCachingDecoder;
import io.cdap.cdap.common.utils.ResettableByteArrayInputStream;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.DatumWriterFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.bouncycastle.util.Strings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link MetricsMessageAggregator}
 */
public class MetricsMessageAggregatorTest {

  private MetricsMessageAggregator aggregator;
  private Injector injector;

  @Before
  public void beforeTest() {
    injector = Guice.createInjector(new IOModule());
    aggregator = new MetricsMessageAggregator(
        injector.getInstance(SchemaGenerator.class),
        injector.getInstance(DatumWriterFactory.class),
        injector.getInstance(DatumReaderFactory.class), 1);
  }

  @Test
  public void testAggregateDecodingFailure() throws UnsupportedTypeException {
    Map<String, String> tags = new HashMap<>();
    tags.put("key1", "value1");
    MetricValues metric = new MetricValues(tags, "metric1", 0, 1,
        MetricType.COUNTER);
    TypeToken<MetricValues> metricValueType = TypeToken.of(MetricValues.class);
    Schema schema = injector.getInstance(SchemaGenerator.class)
        .generate(metricValueType.getType());
    byte[] encodedMetric = encodeMetricValues(metric,
        injector.getInstance(DatumWriterFactory.class)
            .create(TypeToken.of(MetricValues.class), schema));
    byte[] invalidPayload = "message".getBytes(StandardCharsets.UTF_8);

    Iterator<Message> input = Arrays.asList(new Message[]{
            getMessage(encodedMetric, "1"),
            getMessage(encodedMetric, "2"),
            getMessage(invalidPayload, "3"),
            getMessage(encodedMetric, "4")})
        .iterator();

    DatumReader<MetricValues> reader = injector.getInstance(
            DatumReaderFactory.class)
        .create(metricValueType, schema);
    Iterator<Message> result = aggregator.aggregate(input);
    Message current = result.next();
    Assert.assertEquals("3", current.getId());
    Assert.assertEquals(2,
        decodeMetricValues(current.getPayload(), reader, schema).getMetrics()
            .iterator().next().getValue());

    current = result.next();
    Assert.assertEquals("3", current.getId());
    Assert.assertEquals("message",
        Strings.fromUTF8ByteArray(current.getPayload()));

    current = result.next();
    Assert.assertEquals("4", current.getId());
    Assert.assertEquals(1,
        decodeMetricValues(current.getPayload(), reader, schema).getMetrics()
            .iterator().next().getValue());
    Assert.assertFalse(result.hasNext());
  }

  private Message getMessage(byte[] payload, String id) {
    return new Message() {
      @Override
      public String getId() {
        return id;
      }

      @Override
      public byte[] getPayload() {
        return payload;
      }
    };
  }

  public static byte[] encodeMetricValues(MetricValues metricValues,
      DatumWriter<MetricValues> writer) {
    ByteArrayOutputStream encOs = new ByteArrayOutputStream(1024);
    Encoder enc = new BinaryEncoder(encOs);
    try {
      writer.encode(metricValues, enc);
      return encOs.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static MetricValues decodeMetricValues(byte[] bytes,
      DatumReader<MetricValues> reader, Schema schema) {
    HashMap<String, String> cachemap = new HashMap<>();
    ResettableByteArrayInputStream decOs = new ResettableByteArrayInputStream();
    StringCachingDecoder decoder = new StringCachingDecoder(
        new BinaryDecoder(decOs), cachemap);
    decOs.reset(bytes);
    try {
      return reader.read(decoder, schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

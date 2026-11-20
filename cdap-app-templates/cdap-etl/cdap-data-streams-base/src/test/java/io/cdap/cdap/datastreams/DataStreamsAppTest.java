/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.datastreams;

import io.cdap.cdap.api.app.ApplicationConfigurer;
import io.cdap.cdap.api.app.ApplicationContext;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DataStreamsApp}.
 */
public class DataStreamsAppTest {

  @Test
  public void testConfigureUsesDefaultDescriptionWhenNull() {
    DataStreamsConfig config = createConfig(null);
    AtomicReference<String> capturedDescription = new AtomicReference<>();
    AtomicReference<Spark> capturedSpark = new AtomicReference<>();
    ApplicationConfigurer configurer = createConfigurer(capturedDescription, capturedSpark);
    ApplicationContext<DataStreamsConfig> context = createContext(config);

    DataStreamsApp app = new DataStreamsApp();
    app.configure(configurer, context);

    Assert.assertEquals("Data Streams Application", capturedDescription.get());
    Assert.assertNotNull(capturedSpark.get());
  }

  @Test
  public void testConfigureUsesCustomDescriptionWhenProvided() {
    String customDescription = "Custom Streaming Pipeline";
    DataStreamsConfig config = createConfig(customDescription);
    AtomicReference<String> capturedDescription = new AtomicReference<>();
    AtomicReference<Spark> capturedSpark = new AtomicReference<>();
    ApplicationConfigurer configurer = createConfigurer(capturedDescription, capturedSpark);
    ApplicationContext<DataStreamsConfig> context = createContext(config);

    DataStreamsApp app = new DataStreamsApp();
    app.configure(configurer, context);

    Assert.assertTrue(app.isUpdateSupported());
    Assert.assertEquals(customDescription, capturedDescription.get());
    Assert.assertNotNull(capturedSpark.get());
  }

  private DataStreamsConfig createConfig(String description) {
    DataStreamsConfig.Builder builder = DataStreamsConfig.builder()
        .addStage(new ETLStage(
            "src", new ETLPlugin("mockSrc", StreamingSource.PLUGIN_TYPE, Collections.emptyMap())))
        .addStage(new ETLStage(
            "sink", new ETLPlugin("mockSink", BatchSink.PLUGIN_TYPE, Collections.emptyMap())))
        .addConnection("src", "sink")
        .setBatchInterval("1s");
    if (description != null) {
      builder.setDescription(description);
    }
    return builder.build();
  }

  private ApplicationConfigurer createConfigurer(
      AtomicReference<String> capturedDescription, AtomicReference<Spark> capturedSpark) {
    return (ApplicationConfigurer) Proxy.newProxyInstance(
        ApplicationConfigurer.class.getClassLoader(),
        new Class<?>[]{ApplicationConfigurer.class},
        (proxy, method, args) -> {
          String name = method.getName();
          if ("setDescription".equals(name)) {
            capturedDescription.set((String) args[0]);
            return null;
          }
          if ("addSpark".equals(name)) {
            capturedSpark.set((Spark) args[0]);
            return null;
          }
          if ("getDeployedNamespace".equals(name)) {
            return "default";
          }
          if ("usePlugin".equals(name)) {
            String pluginId = (String) args[2];
            if ("src".equals(pluginId)) {
              return new StreamingSource<Object>() {
                @Override
                public JavaDStream<Object> getStream(StreamingContext context) {
                  return null;
                }
              };
            }
            if ("sink".equals(pluginId)) {
              return new BatchSink<Object, Object, Object>() {
                @Override
                public void prepareRun(BatchSinkContext context) {
                }
              };
            }
            return null;
          }
          if (method.getReturnType() == boolean.class) {
            return false;
          }
          return null;
        });
  }

  @SuppressWarnings("unchecked")
  private ApplicationContext<DataStreamsConfig> createContext(DataStreamsConfig config) {
    return (ApplicationContext<DataStreamsConfig>) Proxy.newProxyInstance(
        ApplicationContext.class.getClassLoader(),
        new Class<?>[]{ApplicationContext.class},
        (proxy, method, args) -> {
          if ("getConfig".equals(method.getName())) {
            return config;
          }
          return null;
        });
  }
}

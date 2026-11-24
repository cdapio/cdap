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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link RDDCollection}.
 */
public class RDDCollectionTest {

  private static JavaSparkContext jsc;

  @BeforeClass
  public static void setUpClass() {
    SparkConf conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("RDDCollectionTest")
        .set("spark.ui.enabled", "false");
    jsc = new JavaSparkContext(conf);
  }

  @AfterClass
  public static void tearDownClass() {
    if (jsc != null) {
      jsc.close();
      jsc = null;
    }
  }

  @Test
  public void testCreateStoreTaskSparkSinkSuccessAndExceptionPropagation() {
    JavaSparkExecutionContext sec = createMockExecutionContext();
    SQLContext sqlContext = new SQLContext(jsc);
    JavaRDD<String> rdd = jsc.parallelize(Collections.singletonList("row1"));
    RDDCollection<String> collection = new RDDCollection<>(
        sec, FunctionCache.Factory.newInstance(false), jsc, sqlContext, null, null, rdd);

    StageSpec stageSpec = StageSpec.builder(
        "sinkStage", new PluginSpec("sparksink", "mockSink", Collections.emptyMap(), null)).build();

    AtomicBoolean sinkExecuted = new AtomicBoolean(false);
    SparkSink<String> okSink = new SparkSink<String>() {
      @Override
      public void prepareRun(SparkPluginContext context) {
      }

      @Override
      public void run(SparkExecutionPluginContext context, JavaRDD<String> input) {
        sinkExecuted.set(true);
      }
    };
    collection.createStoreTask(stageSpec, okSink).run();
    Assert.assertTrue(sinkExecuted.get());

    IOException checkedError = new IOException("checked spark sink failure");
    SparkSink<String> checkedFailingSink = new SparkSink<String>() {
      @Override
      public void prepareRun(SparkPluginContext context) {
      }

      @Override
      public void run(SparkExecutionPluginContext context, JavaRDD<String> input) throws Exception {
        throw checkedError;
      }
    };
    try {
      collection.createStoreTask(stageSpec, checkedFailingSink).run();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(checkedError, actual.getCause());
    }

    IllegalStateException uncheckedError = new IllegalStateException("unchecked spark sink failure");
    SparkSink<String> uncheckedFailingSink = new SparkSink<String>() {
      @Override
      public void prepareRun(SparkPluginContext context) {
      }

      @Override
      public void run(SparkExecutionPluginContext context, JavaRDD<String> input) {
        throw uncheckedError;
      }
    };
    try {
      collection.createStoreTask(stageSpec, uncheckedFailingSink).run();
      Assert.fail("Expected IllegalStateException directly");
    } catch (IllegalStateException actual) {
      Assert.assertSame(uncheckedError, actual);
    }
  }

  private JavaSparkExecutionContext createMockExecutionContext() {
    ApplicationSpecification appSpec = (ApplicationSpecification) Proxy.newProxyInstance(
        ApplicationSpecification.class.getClassLoader(),
        new Class<?>[]{ApplicationSpecification.class},
        (proxy, method, args) -> "name".equals(method.getName())
            || "getName".equals(method.getName()) ? "testApp" : null);

    Metrics metrics = new SerializableNoOpMetrics();

    return new BatchSQLEngineAdapterTest().new MockJavaSparkExecutionContext() {
      @Override
      public ApplicationSpecification getApplicationSpecification() {
        return appSpec;
      }

      @Override
      public Map<String, String> getRuntimeArguments() {
        return Collections.emptyMap();
      }

      @Override
      public String getNamespace() {
        return "default";
      }

      @Override
      public Metrics getMetrics() {
        return metrics;
      }
    };
  }

  private static final class SerializableNoOpMetrics implements Metrics, Serializable {

    @Override
    public void count(String metricName, int delta) {
    }

    @Override
    public void countLong(String metricName, long delta) {
    }

    @Override
    public void gauge(String metricName, long value) {
    }

    @Override
    public Metrics child(Map<String, String> tags) {
      return this;
    }

    @Override
    public Map<String, String> getTags() {
      return Collections.emptyMap();
    }
  }
}

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

import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.engine.sql.SQLEngine;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link SQLEngineCollection}.
 */
public class SQLEngineCollectionTest {

  private static JavaSparkContext jsc;

  @BeforeClass
  public static void setUpClass() {
    SparkConf conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("SQLEngineCollectionTest")
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
    StageSpec stageSpec = StageSpec.builder(
        "sink1", new PluginSpec("sparksink", "mockSink", Collections.emptyMap(), null)).build();
    SparkSink<String> dummySink = new SparkSink<String>() {
      @Override
      public void prepareRun(SparkPluginContext context) {
      }

      @Override
      public void run(SparkExecutionPluginContext context, JavaRDD<String> input) {
      }
    };

    AtomicBoolean ran = new AtomicBoolean(false);
    SQLEngineCollection<String> okCollection = createWithStoreRunnable(() -> ran.set(true), null);
    okCollection.createStoreTask(stageSpec, dummySink).run();
    Assert.assertTrue(ran.get());

    IllegalArgumentException uncheckedError = new IllegalArgumentException("unchecked store error");
    SQLEngineCollection<String> uncheckedCollection = createWithStoreRunnable(() -> {
      throw uncheckedError;
    }, null);
    try {
      uncheckedCollection.createStoreTask(stageSpec, dummySink).run();
      Assert.fail("Expected IllegalArgumentException directly");
    } catch (IllegalArgumentException actual) {
      Assert.assertSame(uncheckedError, actual);
    }

    IOException checkedError = new IOException("checked store error");
    SQLEngineCollection<String> checkedCollection = createWithStoreRunnable(
        () -> sneakyThrow(checkedError), null);
    try {
      checkedCollection.createStoreTask(stageSpec, dummySink).run();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(checkedError, actual.getCause());
    }
  }

  @Test
  public void testTryMultiStoreDirectPropagatesInterruptedException() {
    StageSpec sinkStage = StageSpec.builder(
        "sink1", new PluginSpec(BatchSink.PLUGIN_TYPE, "mockSink", Collections.emptyMap(), null))
        .build();
    PipelinePhase phase = PipelinePhase.builder(Collections.singleton(BatchSink.PLUGIN_TYPE))
        .addStage(sinkStage)
        .build();
    PhaseSpec phaseSpec = new PhaseSpec("phase-1", phase, Collections.emptyMap(), false, false);

    SQLEngine<?, ?, ?, ?> noOpEngine = (SQLEngine<?, ?, ?, ?>) Proxy.newProxyInstance(
        SQLEngine.class.getClassLoader(),
        new Class<?>[]{SQLEngine.class},
        (proxy, method, args) -> Collections.emptySet());
    JavaSparkExecutionContext sec = createMockContext();
    CountDownLatch unblockFuture = new CountDownLatch(1);

    BatchSQLEngineAdapter blockingAdapter = new BatchSQLEngineAdapter(
        "engine", noOpEngine, sec, jsc, Collections.emptyMap(), false) {
      @Override
      public <T> CompletableFuture<T> submitTask(Supplier<T> supplier) {
        CompletableFuture<T> uncompleted = new CompletableFuture<>();
        // Interrupt current thread before SQLEngineCollection calls future.get()
        Thread.currentThread().interrupt();
        return uncompleted;
      }
    };

    try {
      SQLEngineCollection<String> collection =
          createWithStoreRunnable(() -> { }, blockingAdapter);
      try {
        collection.tryMultiStoreDirect(phaseSpec, Collections.singleton("sink1"));
        Assert.fail("Expected RuntimeException wrapping InterruptedException");
      } catch (RuntimeException actual) {
        Assert.assertTrue(actual.getCause() instanceof InterruptedException);
      }
    } finally {
      Thread.interrupted();
      unblockFuture.countDown();
      blockingAdapter.close();
    }
  }

  @SuppressWarnings("unchecked")
  private SQLEngineCollection<String> createWithStoreRunnable(
      Runnable storeRunnable, BatchSQLEngineAdapter adapter) {
    BatchCollection<String> localCollection = (BatchCollection<String>) Proxy.newProxyInstance(
        BatchCollection.class.getClassLoader(),
        new Class<?>[]{BatchCollection.class},
        (proxy, method, args) -> {
          if ("createStoreTask".equals(method.getName())) {
            return storeRunnable;
          }
          return null;
        });

    return new SQLEngineCollection<>(
        null, null, jsc, null, null, null, localCollection, "ds", adapter, null);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable throwable) throws E {
    throw (E) throwable;
  }

  private JavaSparkExecutionContext createMockContext() {
    Metrics metrics = new Metrics() {
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
    };

    return new BatchSQLEngineAdapterTest().new MockJavaSparkExecutionContext() {
      @Override
      public Metrics getMetrics() {
        return metrics;
      }
    };
  }
}

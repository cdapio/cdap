/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import org.apache.twill.api.RunId;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.spark.util.TaskFailureListener;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for metrics buffering, task completion, failure handling,
 * and ThreadLocal isolation in {@link SparkRuntimeContext}.
 */
public class SparkRuntimeContextMetricsTest {

  private SparkRuntimeContext sparkRuntimeContext;
  private final Map<Map<String, String>, MetricsContext> childMetricsContexts = new ConcurrentHashMap<>();
  private final Map<String, List<Long>> recordedCounts = new ConcurrentHashMap<>();

  @Before
  public void setUp() {
    recordedCounts.clear();
    childMetricsContexts.clear();

    Program program = Mockito.mock(Program.class);
    ApplicationSpecification appSpec = Mockito.mock(ApplicationSpecification.class);
    SparkSpecification sparkSpec = new SparkSpecification(
        "TestSparkClass", "testSpark", "test description", "TestMainClass",
        Collections.emptySet(), Collections.emptyMap(), null, null, null,
        Collections.emptyList(), Collections.emptyMap()
    );
    ProgramId programId = new ProgramId("default", "testApp", ProgramType.SPARK, "testSpark");

    Mockito.when(program.getId()).thenReturn(programId);
    Mockito.when(program.getName()).thenReturn("testSpark");
    Mockito.when(program.getType()).thenReturn(ProgramType.SPARK);
    Mockito.when(program.getApplicationSpecification()).thenReturn(appSpec);
    Mockito.when(appSpec.getSpark()).thenReturn(Collections.singletonMap("testSpark", sparkSpec));

    MetricsCollectionService metricsCollectionService = Mockito.mock(MetricsCollectionService.class);
    Mockito.when(metricsCollectionService.getContext(Mockito.anyMap())).thenAnswer(invocation -> {
      Map<String, String> tags = (Map<String, String>) invocation.getArguments()[0];
      return childMetricsContexts.computeIfAbsent(tags, this::createMockMetricsContext);
    });

    ProgramOptions programOptions = Mockito.mock(ProgramOptions.class);
    Arguments systemArgs = Mockito.mock(Arguments.class);
    Arguments userArgs = Mockito.mock(Arguments.class);
    RunId runId = RunIds.generate();
    Mockito.when(systemArgs.getOption(ProgramOptionConstants.RUN_ID)).thenReturn(runId.getId());
    Mockito.when(systemArgs.getOption(ProgramOptionConstants.ARTIFACT_ID)).thenReturn("default:testArtifact:1.0.0");
    Mockito.when(systemArgs.iterator()).thenReturn(Collections.emptyIterator());
    Mockito.when(userArgs.iterator()).thenReturn(Collections.emptyIterator());
    Mockito.when(programOptions.getArguments()).thenReturn(systemArgs);
    Mockito.when(programOptions.getUserArguments()).thenReturn(userArgs);
    Mockito.when(programOptions.getProgramId()).thenReturn(programId);
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = new Configuration();

    sparkRuntimeContext = new SparkRuntimeContext(
      hConf, program, programOptions, cConf, "localhost",
      Mockito.mock(TransactionSystemClient.class),
      Mockito.mock(DatasetFramework.class),
      metricsCollectionService,
      null, null,
      Mockito.mock(SecureStore.class),
      Mockito.mock(SecureStoreManager.class),
      Mockito.mock(AccessEnforcer.class),
      Mockito.mock(AuthenticationContext.class),
      Mockito.mock(MessagingService.class),
      Mockito.mock(ServiceAnnouncer.class),
      Mockito.mock(PluginFinder.class),
      Mockito.mock(LocationFactory.class),
      Mockito.mock(MetadataReader.class),
      Mockito.mock(MetadataPublisher.class),
      Mockito.mock(NamespaceQueryAdmin.class),
      Mockito.mock(FieldLineageWriter.class),
      Mockito.mock(RemoteClientFactory.class),
      Mockito.mock(Closeable.class),
      Mockito.mock(AppStateStoreProvider.class)
    );
  }

  @After
  public void tearDown() {
    unsetSparkTaskContext();
  }

  private MetricsContext createMockMetricsContext(Map<String, String> tags) {
    MetricsContext mc = Mockito.mock(MetricsContext.class);
    Mockito.when(mc.getTags()).thenReturn(tags);

    Mockito.doAnswer(invocation -> {
      String name = (String) invocation.getArguments()[0];
      long count = (Long) invocation.getArguments()[1];
      recordMetric(tags, name, count);
      return null;
    }).when(mc).increment(Mockito.anyString(), Mockito.anyLong());

    Mockito.doAnswer(invocation -> {
      String name = (String) invocation.getArguments()[0];
      long value = (Long) invocation.getArguments()[1];
      recordMetric(tags, name, value);
      return null;
    }).when(mc).gauge(Mockito.anyString(), Mockito.anyLong());

    Mockito.when(mc.childContext(Mockito.anyMap())).thenAnswer(invocation -> {
      Map<String, String> childTags = new HashMap<>(tags);
      childTags.putAll((Map<String, String>) invocation.getArguments()[0]);
      return childMetricsContexts.computeIfAbsent(childTags, this::createMockMetricsContext);
    });

    Mockito.when(mc.childContext(Mockito.anyString(), Mockito.anyString())).thenAnswer(invocation -> {
      Map<String, String> childTags = new HashMap<>(tags);
      childTags.put((String) invocation.getArguments()[0], (String) invocation.getArguments()[1]);
      return childMetricsContexts.computeIfAbsent(childTags, this::createMockMetricsContext);
    });

    return mc;
  }

  private void recordMetric(Map<String, String> tags, String metricName, Long value) {
    String key = metricName + "|" + tags.getOrDefault(Constants.Metrics.Tag.SPARK_PARTITION, "none")
      + "|" + tags.getOrDefault(Constants.Metrics.Tag.SPARK_ATTEMPT, "none");
    recordedCounts.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
  }

  private static void setSparkTaskContext(TaskContext tc) {
    try {
      Method m = TaskContext$.MODULE$.getClass().getDeclaredMethod("setTaskContext", TaskContext.class);
      m.setAccessible(true);
      m.invoke(TaskContext$.MODULE$, tc);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set Spark TaskContext", e);
    }
  }

  private static void unsetSparkTaskContext() {
    try {
      Method m = TaskContext$.MODULE$.getClass().getDeclaredMethod("unset");
      m.setAccessible(true);
      m.invoke(TaskContext$.MODULE$);
    } catch (Exception e) {
      throw new RuntimeException("Failed to unset Spark TaskContext", e);
    }
  }

  private static class MockTaskContextHolder {
    final TaskContext taskContext;
    final List<TaskCompletionListener> completionListeners = new ArrayList<>();
    final List<TaskFailureListener> failureListeners = new ArrayList<>();
    boolean isInterrupted = false;

    MockTaskContextHolder(int partitionId, int attemptNumber) {
      taskContext = Mockito.mock(TaskContext.class);
      Mockito.when(taskContext.partitionId()).thenReturn(partitionId);
      Mockito.when(taskContext.attemptNumber()).thenReturn(attemptNumber);
      Mockito.when(taskContext.isInterrupted()).thenAnswer(inv -> isInterrupted);

      Mockito.doAnswer(inv -> {
        completionListeners.add((TaskCompletionListener) inv.getArguments()[0]);
        return taskContext;
      }).when(taskContext).addTaskCompletionListener(Mockito.any(TaskCompletionListener.class));

      Mockito.doAnswer(inv -> {
        failureListeners.add((TaskFailureListener) inv.getArguments()[0]);
        return taskContext;
      }).when(taskContext).addTaskFailureListener(Mockito.any(TaskFailureListener.class));
    }

    void succeed() {
      for (TaskCompletionListener listener : completionListeners) {
        listener.onTaskCompletion(taskContext);
      }
    }

    void fail(Throwable error) {
      for (TaskFailureListener listener : failureListeners) {
        listener.onTaskFailure(taskContext, error);
      }
      for (TaskCompletionListener listener : completionListeners) {
        listener.onTaskCompletion(taskContext);
      }
    }
  }

  @Test
  public void testMetricsBufferingAndSuccessFlush() {
    MockTaskContextHolder holder = new MockTaskContextHolder(0, 0);
    setSparkTaskContext(holder.taskContext);

    // Call count on target metrics
    sparkRuntimeContext.count("user.stage1.records.in", 100);
    sparkRuntimeContext.count("user.stage1.records.out", 200);
    sparkRuntimeContext.count("user.stage1.records.error", 5);
    sparkRuntimeContext.count("user.stage1.records.alert", 2);

    // Verify immediate flat metrics were emitted
    Assert.assertEquals(Collections.singletonList(100L), recordedCounts.get("user.stage1.records.in|none|none"));
    Assert.assertEquals(Collections.singletonList(200L), recordedCounts.get("user.stage1.records.out|none|none"));
    Assert.assertEquals(Collections.singletonList(5L), recordedCounts.get("user.stage1.records.error|none|none"));
    Assert.assertEquals(Collections.singletonList(2L), recordedCounts.get("user.stage1.records.alert|none|none"));

    // Raw metrics should NOT be emitted yet
    Assert.assertNull(recordedCounts.get("user.stage1.records.in.raw|0|0"));
    Assert.assertNull(recordedCounts.get("user.stage1.records.out.raw|0|0"));

    // Simulate task success
    holder.succeed();

    // Verify raw metrics are now flushed with partition and attempt tags
    Assert.assertEquals(Collections.singletonList(100L), recordedCounts.get("user.stage1.records.in.raw|0|0"));
    Assert.assertEquals(Collections.singletonList(200L), recordedCounts.get("user.stage1.records.out.raw|0|0"));
    Assert.assertEquals(Collections.singletonList(5L), recordedCounts.get("user.stage1.records.error.raw|0|0"));
    Assert.assertEquals(Collections.singletonList(2L), recordedCounts.get("user.stage1.records.alert.raw|0|0"));
  }

  @Test
  public void testNonTargetMetricsNotBufferedAsRaw() {
    MockTaskContextHolder holder = new MockTaskContextHolder(0, 0);
    setSparkTaskContext(holder.taskContext);

    sparkRuntimeContext.count("system.process.time", 500);

    // Emitted immediately as flat metric
    Assert.assertEquals(Collections.singletonList(500L), recordedCounts.get("system.process.time|none|none"));

    // Complete task
    holder.succeed();

    // Raw metric should never be emitted for non-target suffix
    Assert.assertNull(recordedCounts.get("system.process.time.raw|0|0"));
  }

  @Test
  public void testTaskFailureDiscardsRawMetrics() {
    MockTaskContextHolder holder = new MockTaskContextHolder(0, 0);
    setSparkTaskContext(holder.taskContext);

    sparkRuntimeContext.count("user.stage1.records.out", 50);

    // Immediate flat metric was emitted
    Assert.assertEquals(Collections.singletonList(50L), recordedCounts.get("user.stage1.records.out|none|none"));

    // Simulate task failure (exception)
    holder.fail(new RuntimeException("Simulated task exception"));

    // Raw metric should NOT be emitted on failure
    Assert.assertNull(recordedCounts.get("user.stage1.records.out.raw|0|0"));
  }

  @Test
  public void testTaskInterruptionDiscardsRawMetrics() {
    MockTaskContextHolder holder = new MockTaskContextHolder(0, 0);
    setSparkTaskContext(holder.taskContext);

    sparkRuntimeContext.count("user.stage1.records.out", 50);

    // Task was interrupted (e.g. killed by driver)
    holder.isInterrupted = true;
    holder.succeed();

    // Raw metric should NOT be emitted on interruption
    Assert.assertNull(recordedCounts.get("user.stage1.records.out.raw|0|0"));
  }

  @Test
  public void testMultiThreadedIsolation() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);

    Future<Void> task1 = executor.submit(() -> {
      MockTaskContextHolder holder1 = new MockTaskContextHolder(0, 1);
      setSparkTaskContext(holder1.taskContext);
      startLatch.await();

      for (int i = 0; i < 100; i++) {
        sparkRuntimeContext.count("user.stage1.records.out", 1);
      }
      holder1.succeed();
      unsetSparkTaskContext();
      return null;
    });

    Future<Void> task2 = executor.submit(() -> {
      MockTaskContextHolder holder2 = new MockTaskContextHolder(1, 0);
      setSparkTaskContext(holder2.taskContext);
      startLatch.await();

      for (int i = 0; i < 250; i++) {
        sparkRuntimeContext.count("user.stage1.records.out", 1);
      }
      holder2.succeed();
      unsetSparkTaskContext();
      return null;
    });

    startLatch.countDown();
    task1.get(10, TimeUnit.SECONDS);
    task2.get(10, TimeUnit.SECONDS);
    executor.shutdown();

    // Verify task 1 flushed 100 for partition 0, attempt 1
    Assert.assertEquals(Collections.singletonList(100L), recordedCounts.get("user.stage1.records.out.raw|0|1"));
    // Verify task 2 flushed 250 for partition 1, attempt 0
    Assert.assertEquals(Collections.singletonList(250L), recordedCounts.get("user.stage1.records.out.raw|1|0"));
  }

  @Test
  public void testThreadReuseBufferCleanup() {
    // Run Task 1 on current thread
    MockTaskContextHolder holder1 = new MockTaskContextHolder(0, 0);
    setSparkTaskContext(holder1.taskContext);
    sparkRuntimeContext.count("user.stage1.records.out", 50);
    holder1.succeed();
    unsetSparkTaskContext();

    Assert.assertEquals(Collections.singletonList(50L), recordedCounts.get("user.stage1.records.out.raw|0|0"));

    // Run Task 2 on the same thread
    MockTaskContextHolder holder2 = new MockTaskContextHolder(1, 0);
    setSparkTaskContext(holder2.taskContext);
    sparkRuntimeContext.count("user.stage1.records.out", 75);
    holder2.succeed();
    unsetSparkTaskContext();

    // Task 2 must have exactly 75, not 50 + 75
    Assert.assertEquals(Collections.singletonList(75L), recordedCounts.get("user.stage1.records.out.raw|1|0"));
  }

  @Test
  public void testDriverExecutionWithoutTaskContext() {
    // When running on driver, TaskContext.get() is null
    unsetSparkTaskContext();

    sparkRuntimeContext.count("user.stage1.records.out", 20);

    // Emitted immediately as flat metric
    Assert.assertEquals(Collections.singletonList(20L), recordedCounts.get("user.stage1.records.out|none|none"));

    // No raw metric emitted
    Assert.assertNull(recordedCounts.get("user.stage1.records.out.raw|none|none"));
  }

  @Test
  public void testGaugeBuffering() {
    MockTaskContextHolder holder = new MockTaskContextHolder(0, 0);
    setSparkTaskContext(holder.taskContext);

    // For gauge, value is set rather than summed
    sparkRuntimeContext.gauge("user.stage1.records.out", 10);
    sparkRuntimeContext.gauge("user.stage1.records.out", 42);

    holder.succeed();

    // Raw gauge should have latest value
    Assert.assertEquals(Collections.singletonList(42L), recordedCounts.get("user.stage1.records.out.raw|0|0"));
  }
}

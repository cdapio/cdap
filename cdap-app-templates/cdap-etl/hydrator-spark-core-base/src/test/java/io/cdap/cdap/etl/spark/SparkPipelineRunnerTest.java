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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.TaskLocalizationContext;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.SparkExecutionContext;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.api.spark.dynamic.SparkInterpreter;
import io.cdap.cdap.api.workflow.WorkflowInfo;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SparkPipelineRunner}.
 */
public class SparkPipelineRunnerTest {

  @Test
  public void testExecuteSinkRunnablesParallelSuccess() {
    AtomicInteger ranCount = new AtomicInteger(0);
    TestSparkPipelineRunner runner = new TestSparkPipelineRunner();
    JavaSparkExecutionContext sec = createContext(
        Collections.singletonMap("pipeline.spark.parallel.sinks.enabled", "true"));

    runner.runSinks(sec, Arrays.asList(ranCount::incrementAndGet, ranCount::incrementAndGet));
    Assert.assertEquals(2, ranCount.get());
  }

  @Test
  public void testExecuteSinkRunnablesParallelPropagatesUncheckedException() {
    IllegalStateException expected = new IllegalStateException("parallel sink unchecked failure");
    TestSparkPipelineRunner runner = new TestSparkPipelineRunner();
    JavaSparkExecutionContext sec = createContext(
        Collections.singletonMap("pipeline.spark.parallel.sinks.enabled", "true"));

    try {
      runner.runSinks(sec, Collections.singletonList(() -> {
        throw expected;
      }));
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException actual) {
      Assert.assertSame(expected, actual);
    }
  }

  @Test
  public void testExecuteSinkRunnablesParallelWrapsCheckedThrowable() {
    IOException checkedError = new IOException("parallel sink checked failure");
    TestSparkPipelineRunner runner = new TestSparkPipelineRunner();
    JavaSparkExecutionContext sec = createContext(
        Collections.singletonMap("pipeline.spark.parallel.sinks.enabled", "true"));

    try {
      runner.runSinks(sec, Collections.singletonList(() -> sneakyThrow(checkedError)));
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(checkedError, actual.getCause());
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable throwable) throws E {
    throw (E) throwable;
  }

  private JavaSparkExecutionContext createContext(Map<String, String> runtimeArgs) {
    return new JavaSparkExecutionContext() {
      @Override
      public SparkSpecification getSpecification() {
        return null;
      }

      @Override
      public long getLogicalStartTime() {
        return 0;
      }

      @Override
      public long getTerminationTime() {
        return 0;
      }

      @Override
      public ServiceDiscoverer getServiceDiscoverer() {
        return null;
      }

      @Override
      public Metrics getMetrics() {
        return null;
      }

      @Override
      public PluginContext getPluginContext() {
        return null;
      }

      @Override
      public SecureStore getSecureStore() {
        return null;
      }

      @Override
      public MessagingContext getMessagingContext() {
        return null;
      }

      @Override
      public TaskLocalizationContext getLocalizationContext() {
        return null;
      }

      @Override
      public <K, V> JavaPairRDD<K, V> fromDataset(
          String datasetName, Map<String, String> arguments,
          @Nullable Iterable<? extends Split> splits) {
        return null;
      }

      @Override
      public <K, V> JavaPairRDD<K, V> fromDataset(
          String namespace, String datasetName, Map<String, String> arguments,
          @Nullable Iterable<? extends Split> splits) {
        return null;
      }

      @Override
      public <K, V> void saveAsDataset(
          JavaPairRDD<K, V> rdd, String datasetName, Map<String, String> arguments) {
      }

      @Override
      public <K, V> void saveAsDataset(
          JavaPairRDD<K, V> rdd, String namespace, String datasetName,
          Map<String, String> arguments) {
      }

      @Override
      public void execute(TxRunnable runnable) {
      }

      @Override
      public void execute(int timeoutInSeconds, TxRunnable runnable) {
      }

      @Override
      public SparkInterpreter createInterpreter() {
        return null;
      }

      @Override
      public SparkExecutionContext getSparkExecutionContext() {
        return null;
      }

      @Override
      public ApplicationSpecification getApplicationSpecification() {
        return null;
      }

      @Override
      public Map<String, String> getRuntimeArguments() {
        return runtimeArgs;
      }

      @Override
      public String getClusterName() {
        return null;
      }

      @Override
      public String getNamespace() {
        return "default";
      }

      @Override
      public RunId getRunId() {
        return null;
      }

      @Override
      public Admin getAdmin() {
        return null;
      }

      @Override
      public DataTracer getDataTracer(String dataTracerName) {
        return null;
      }

      @Nullable
      @Override
      public TriggeringScheduleInfo getTriggeringScheduleInfo() {
        return null;
      }

      @Override
      public void record(Collection<? extends Operation> operations) {
      }

      @Override
      public void flushLineage() {
      }

      @Override
      public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) {
        return Collections.emptyMap();
      }

      @Override
      public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
        return null;
      }

      @Override
      public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {
      }

      @Override
      public void addTags(MetadataEntity metadataEntity, String... tags) {
      }

      @Override
      public void addTags(MetadataEntity metadataEntity, Iterable<String> tags) {
      }

      @Override
      public void removeMetadata(MetadataEntity metadataEntity) {
      }

      @Override
      public void removeProperties(MetadataEntity metadataEntity) {
      }

      @Override
      public void removeProperties(MetadataEntity metadataEntity, String... keys) {
      }

      @Override
      public void removeTags(MetadataEntity metadataEntity) {
      }

      @Override
      public void removeTags(MetadataEntity metadataEntity, String... tags) {
      }

      @Override
      public List<SecureStoreMetadata> list(String namespace) {
        return Collections.emptyList();
      }

      @Override
      public SecureStoreData get(String namespace, String name) {
        return null;
      }

      @Nullable
      @Override
      public WorkflowToken getWorkflowToken() {
        return null;
      }

      @Nullable
      @Override
      public WorkflowInfo getWorkflowInfo() {
        return null;
      }
    };
  }

  private static class TestSparkPipelineRunner extends SparkPipelineRunner {

    void runSinks(JavaSparkExecutionContext sec, Collection<Runnable> sinkRunnables) {
      executeSinkRunnables(sec, sinkRunnables);
    }

    @Override
    protected SparkCollection<RecordInfo<Object>> getSource(
        StageSpec stageSpec, FunctionCache.Factory functionCacheFactory,
        StageStatisticsCollector collector) {
      return null;
    }

    @Override
    protected JavaSparkContext getSparkContext() {
      return null;
    }

    @Override
    protected SparkPairCollection<Object, Object> addJoinKey(
        StageSpec stageSpec, FunctionCache.Factory functionCacheFactory, String inputStageName,
        SparkCollection<Object> inputCollection, StageStatisticsCollector collector) {
      return null;
    }

    @Override
    protected SparkCollection<Object> mergeJoinResults(
        StageSpec stageSpec, FunctionCache.Factory functionCacheFactory,
        SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs,
        StageStatisticsCollector collector) {
      return null;
    }
  }
}

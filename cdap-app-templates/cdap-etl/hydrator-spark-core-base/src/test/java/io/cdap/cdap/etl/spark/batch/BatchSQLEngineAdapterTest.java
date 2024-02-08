/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.TaskLocalizationContext;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataException;
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
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.engine.SQLEngineJob;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BatchSQLEngineAdapterTest {

  StageMetrics stageMetrics;
  Map<Long, Integer> invocationCounts;

  @Before
  public void setUp() {
    invocationCounts = new HashMap<>();

    stageMetrics = new StageMetrics() {
      @Override
      public void count(String metricName, int delta) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public void countLong(String metricName, long delta) {
        invocationCounts.compute(delta, (k, v) -> (v == null) ? 1 : v + 1);
      }

      @Override
      public void gauge(String metricName, long value) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public void pipelineCount(String metricName, int delta) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public void pipelineGauge(String metricName, long value) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public Metrics child(Map<String, String> tags) {
        throw new UnsupportedOperationException("not implemented");
      }

      @Override
      public Map<String, String> getTags() {
        throw new UnsupportedOperationException("not implemented");
      }
    };
  }

  @Test
  public void testCountStageMetrics() {
    // Answer should be INTEGER.MAX_VALUE called 2 times, 123456 called 1 time.
    long count = (long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE + (long) 123456;

    BatchSQLEngineAdapter.countStageMetrics(stageMetrics, "some-metric", count);
    Assert.assertEquals(1, invocationCounts.size());
    Assert.assertEquals(1, (int) invocationCounts.get(count));

    BatchSQLEngineAdapter.countStageMetrics(stageMetrics, "some-metric", 123456);
    Assert.assertEquals(2, invocationCounts.size());
    Assert.assertEquals(1, (int) invocationCounts.get(count));
    Assert.assertEquals(1, (int) invocationCounts.get(123456L));

    BatchSQLEngineAdapter.countStageMetrics(stageMetrics, "some-metric", 123456);
    Assert.assertEquals(2, invocationCounts.size());
    Assert.assertEquals(1, (int) invocationCounts.get(count));
    Assert.assertEquals(2, (int) invocationCounts.get(123456L));

    BatchSQLEngineAdapter.countStageMetrics(stageMetrics, "some-metric", 9876);
    Assert.assertEquals(3, invocationCounts.size());
    Assert.assertEquals(1, (int) invocationCounts.get(count));
    Assert.assertEquals(2, (int) invocationCounts.get(123456L));
    Assert.assertEquals(1, (int) invocationCounts.get(9876L));

    BatchSQLEngineAdapter.countStageMetrics(stageMetrics, "some-metric", 0);
    Assert.assertEquals(4, invocationCounts.size());
    Assert.assertEquals(1, (int) invocationCounts.get(count));
    Assert.assertEquals(2, (int) invocationCounts.get(123456L));
    Assert.assertEquals(1, (int) invocationCounts.get(9876L));
    Assert.assertEquals(1, (int) invocationCounts.get(0L));
  }

  @Test
  public void testNonSchemaReturnsEmpty() {
    BatchSQLEngineAdapter batchSQLEngineAdapter = new BatchSQLEngineAdapter(
      "conditonal.connector", null, new MockJavaSparkExecutionContext(),
      new JavaSparkContext("local","mock"), null, false);

    Schema schema = Schema.recordOf(
      "rec",
      Schema.Field.of("bool_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("int_field", Schema.of(Schema.Type.INT)),
      Schema.Field.of("long_field", Schema.of(Schema.Type.LONG))
    );

    StageSpec stageSpec = new StageSpec.Builder("conditonal.connector",
                                                new PluginSpec("mock", "mock", new HashMap<>(), null))
                                        .addInputSchema("conditonal", schema).build();

    Map<String, SparkCollection<Object>> inputs = new HashMap<String, SparkCollection<Object>>() {{
      put("conditonal.connector", null);
    }};

    Optional<SQLEngineJob<SQLDataset>> result =
      batchSQLEngineAdapter.tryRelationalTransform(stageSpec, null, inputs);

    Assert.assertFalse(result.isPresent());
  }

  class MockJavaSparkExecutionContext extends JavaSparkExecutionContext {

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
    public <K, V> JavaPairRDD<K, V> fromDataset(String datasetName, Map<String, String> arguments,
                                                @Nullable Iterable<? extends Split> splits) {
      return null;
    }

    @Override
    public <K, V> JavaPairRDD<K, V> fromDataset(String namespace, String datasetName, Map<String, String> arguments,
                                                @Nullable Iterable<? extends Split> splits) {
      return null;
    }

    @Override
    public <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String datasetName, Map<String, String> arguments) {

    }

    @Override
    public <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String namespace, String datasetName,
                                     Map<String, String> arguments) {

    }

    @Override
    public void execute(TxRunnable runnable) throws TransactionFailureException {

    }

    @Override
    public void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {

    }

    @Override
    public SparkInterpreter createInterpreter() throws IOException {
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
      return null;
    }

    @Override
    public String getClusterName() {
      return null;
    }

    @Override
    public String getNamespace() {
      return null;
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
    public void flushLineage() throws IllegalArgumentException {

    }

    @Override
    public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) throws MetadataException {
      return null;
    }

    @Override
    public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws MetadataException {
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
    public List<SecureStoreMetadata> list(String namespace) throws Exception {
      return null;
    }

    @Override
    public SecureStoreData get(String namespace, String name) throws Exception {
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
  }
}

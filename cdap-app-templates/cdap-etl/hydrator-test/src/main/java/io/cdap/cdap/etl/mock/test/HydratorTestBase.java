/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.test;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.condition.Condition;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.engine.sql.SQLEngine;
import io.cdap.cdap.etl.api.engine.sql.capability.PushCapability;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.etl.api.join.AutoJoiner;
import io.cdap.cdap.etl.api.join.error.JoinError;
import io.cdap.cdap.etl.api.lineage.AccessType;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.sql.engine.dataset.SparkRecordCollection;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.cdap.etl.mock.action.FieldLineageAction;
import io.cdap.cdap.etl.mock.action.FileMoveAction;
import io.cdap.cdap.etl.mock.action.MockAction;
import io.cdap.cdap.etl.mock.alert.NullAlertTransform;
import io.cdap.cdap.etl.mock.alert.TMSAlertPublisher;
import io.cdap.cdap.etl.mock.batch.FilterTransform;
import io.cdap.cdap.etl.mock.batch.IncapableSink;
import io.cdap.cdap.etl.mock.batch.IncapableSource;
import io.cdap.cdap.etl.mock.batch.LookupTransform;
import io.cdap.cdap.etl.mock.batch.MockExternalSink;
import io.cdap.cdap.etl.mock.batch.MockExternalSource;
import io.cdap.cdap.etl.mock.batch.MockRuntimeDatasetSink;
import io.cdap.cdap.etl.mock.batch.MockRuntimeDatasetSource;
import io.cdap.cdap.etl.mock.batch.MockSQLEngine;
import io.cdap.cdap.etl.mock.batch.MockSQLEngineWithCapabilities;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.batch.NodeStatesAction;
import io.cdap.cdap.etl.mock.batch.NullErrorTransform;
import io.cdap.cdap.etl.mock.batch.aggregator.DistinctAggregator;
import io.cdap.cdap.etl.mock.batch.aggregator.DistinctReducibleAggregator;
import io.cdap.cdap.etl.mock.batch.aggregator.FieldCountAggregator;
import io.cdap.cdap.etl.mock.batch.aggregator.FieldCountReducibleAggregator;
import io.cdap.cdap.etl.mock.batch.aggregator.GroupFilterAggregator;
import io.cdap.cdap.etl.mock.batch.aggregator.IdentityAggregator;
import io.cdap.cdap.etl.mock.batch.joiner.DupeFlagger;
import io.cdap.cdap.etl.mock.batch.joiner.MockAutoJoiner;
import io.cdap.cdap.etl.mock.batch.joiner.MockJoiner;
import io.cdap.cdap.etl.mock.condition.MockCondition;
import io.cdap.cdap.etl.mock.connector.FileConnector;
import io.cdap.cdap.etl.mock.spark.Window;
import io.cdap.cdap.etl.mock.spark.compute.StringValueFilterCompute;
import io.cdap.cdap.etl.mock.transform.AllErrorTransform;
import io.cdap.cdap.etl.mock.transform.DoubleTransform;
import io.cdap.cdap.etl.mock.transform.DropNullTransform;
import io.cdap.cdap.etl.mock.transform.ExceptionTransform;
import io.cdap.cdap.etl.mock.transform.FieldsPrefixTransform;
import io.cdap.cdap.etl.mock.transform.FilterErrorTransform;
import io.cdap.cdap.etl.mock.transform.FlattenErrorTransform;
import io.cdap.cdap.etl.mock.transform.IdentityTransform;
import io.cdap.cdap.etl.mock.transform.IntValueFilterTransform;
import io.cdap.cdap.etl.mock.transform.NullFieldSplitterTransform;
import io.cdap.cdap.etl.mock.transform.SleepTransform;
import io.cdap.cdap.etl.mock.transform.StringValueFilterTransform;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.TestBase;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Performs common setup logic
 */
public class HydratorTestBase extends TestBase {
  // need to specify each PluginClass so that they can be used outside of this project. If we don't do this,
  // when the plugin jar is created, it will add lib/hydrator-test.jar to the plugin jar.
  // The ArtifactInspector will not examine any library jars for plugins, because it assumes plugins are always
  // .class files in the jar and never in the dependencies, which is normally a reasonable assumption.
  // So since the plugins are in lib/hydrator-test.jar, CDAP won't find any plugins in the jar.
  // To work around, we'll just explicitly specify each plugin.
  private static final Set<PluginClass> BATCH_MOCK_PLUGINS = ImmutableSet.of(
    FieldCountAggregator.PLUGIN_CLASS, IdentityAggregator.PLUGIN_CLASS, GroupFilterAggregator.PLUGIN_CLASS,
    MockJoiner.PLUGIN_CLASS, MockAutoJoiner.PLUGIN_CLASS, MockSQLEngine.PLUGIN_CLASS,
    MockSQLEngineWithCapabilities.PLUGIN_CLASS, DupeFlagger.PLUGIN_CLASS,
    MockRuntimeDatasetSink.PLUGIN_CLASS, MockRuntimeDatasetSource.PLUGIN_CLASS,
    MockExternalSource.PLUGIN_CLASS, MockExternalSink.PLUGIN_CLASS,
    DoubleTransform.PLUGIN_CLASS, AllErrorTransform.PLUGIN_CLASS, IdentityTransform.PLUGIN_CLASS,
    FieldsPrefixTransform.PLUGIN_CLASS, IntValueFilterTransform.PLUGIN_CLASS,
    StringValueFilterTransform.PLUGIN_CLASS, FilterTransform.PLUGIN_CLASS, DropNullTransform.PLUGIN_CLASS,
    MockAction.PLUGIN_CLASS, FileMoveAction.PLUGIN_CLASS, FieldLineageAction.PLUGIN_CLASS,
    StringValueFilterCompute.PLUGIN_CLASS, FlattenErrorTransform.PLUGIN_CLASS, FilterErrorTransform.PLUGIN_CLASS,
    NullFieldSplitterTransform.PLUGIN_CLASS, TMSAlertPublisher.PLUGIN_CLASS, NullAlertTransform.PLUGIN_CLASS,
    MockCondition.PLUGIN_CLASS, MockSource.PLUGIN_CLASS, MockSink.PLUGIN_CLASS,
    DistinctReducibleAggregator.PLUGIN_CLASS, FieldCountReducibleAggregator.PLUGIN_CLASS,
    FileConnector.PLUGIN_CLASS, IncapableSource.PLUGIN_CLASS, IncapableSink.PLUGIN_CLASS,
    LookupTransform.PLUGIN_CLASS, SleepTransform.PLUGIN_CLASS, NodeStatesAction.PLUGIN_CLASS,
    DistinctAggregator.PLUGIN_CLASS, NullErrorTransform.PLUGIN_CLASS, ExceptionTransform.PLUGIN_CLASS
  );
  private static final Set<PluginClass> STREAMING_MOCK_PLUGINS = ImmutableSet.of(
    io.cdap.cdap.etl.mock.spark.streaming.MockSource.PLUGIN_CLASS,
    io.cdap.cdap.etl.mock.batch.MockSink.PLUGIN_CLASS,
    io.cdap.cdap.etl.mock.spark.streaming.MockSink.PLUGIN_CLASS, MockExternalSink.PLUGIN_CLASS,
    DoubleTransform.PLUGIN_CLASS, AllErrorTransform.PLUGIN_CLASS, IdentityTransform.PLUGIN_CLASS,
    IntValueFilterTransform.PLUGIN_CLASS, StringValueFilterTransform.PLUGIN_CLASS, DropNullTransform.PLUGIN_CLASS,
    FilterTransform.PLUGIN_CLASS,
    FieldCountAggregator.PLUGIN_CLASS, IdentityAggregator.PLUGIN_CLASS,
    FieldCountReducibleAggregator.PLUGIN_CLASS,
    GroupFilterAggregator.PLUGIN_CLASS, MockJoiner.PLUGIN_CLASS, MockAutoJoiner.PLUGIN_CLASS, DupeFlagger.PLUGIN_CLASS,
    StringValueFilterCompute.PLUGIN_CLASS, Window.PLUGIN_CLASS,
    FlattenErrorTransform.PLUGIN_CLASS, FilterErrorTransform.PLUGIN_CLASS,
    NullFieldSplitterTransform.PLUGIN_CLASS, TMSAlertPublisher.PLUGIN_CLASS, NullAlertTransform.PLUGIN_CLASS,
    SleepTransform.PLUGIN_CLASS
  );
  protected static ArtifactId batchMocksArtifactId;
  protected static ArtifactId streamingMocksArtifactId;

  public HydratorTestBase() {
  }

  protected static void setupBatchArtifacts(ArtifactId artifactId, Class<?> appClass) throws Exception {
    // add the app artifact
    addAppArtifact(artifactId, appClass,
                   BatchSource.class.getPackage().getName(),
                   Action.class.getPackage().getName(),
                   AutoJoiner.class.getPackage().getName(), JoinError.class.getPackage().getName(),
                   Condition.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName(),
                   AccessType.class.getPackage().getName(),
                   InvalidStageException.class.getPackage().getName(),
                   SQLEngine.class.getPackage().getName(),
                   SQLDataset.class.getPackage().getName(),
                   SQLPushRequest.class.getPackage().getName(),
                   PushCapability.class.getPackage().getName(),
                   SparkRecordCollection.class.getPackage().getName(),
                   Connector.class.getPackage().getName(),
                   "org.apache.avro.mapred", "org.apache.avro", "org.apache.avro.generic", "org.apache.avro.io");

    batchMocksArtifactId = new ArtifactId(artifactId.getNamespace(), artifactId.getArtifact() + "-mocks",
                                          artifactId.getVersion());
    // add plugins artifact
    addPluginArtifact(batchMocksArtifactId,
                      artifactId,
                      BATCH_MOCK_PLUGINS,
                      io.cdap.cdap.etl.mock.batch.MockSource.class,
                      io.cdap.cdap.etl.mock.batch.MockSink.class,
                      MockExternalSource.class, MockExternalSink.class, MockSQLEngine.class,
                      MockSQLEngineWithCapabilities.class,
                      DoubleTransform.class, AllErrorTransform.class, IdentityTransform.class,
                      IntValueFilterTransform.class, StringValueFilterTransform.class,
                      FieldCountAggregator.class, FieldsPrefixTransform.class,
                      StringValueFilterCompute.class, NullFieldSplitterTransform.class, NullAlertTransform.class,
                      FileMoveAction.class);
  }

  protected static void setupStreamingArtifacts(ArtifactId artifactId, Class<?> appClass) throws Exception {
    // add the app artifact
    addAppArtifact(artifactId, appClass,
                   StreamingSource.class.getPackage().getName(),
                   Transform.class.getPackage().getName(),
                   AutoJoiner.class.getPackage().getName(), JoinError.class.getPackage().getName(),
                   SparkCompute.class.getPackage().getName(),
                   InvalidStageException.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName(),
                   // have to export this package, otherwise getting ClassCastException in unit test
                   FieldOperation.class.getPackage().getName());


    streamingMocksArtifactId = new ArtifactId(artifactId.getNamespace(), artifactId.getArtifact() + "-mocks", "1.0.0");
    // add plugins artifact
    addPluginArtifact(streamingMocksArtifactId,
                      artifactId,
                      STREAMING_MOCK_PLUGINS,
                      io.cdap.cdap.etl.mock.spark.streaming.MockSource.class,
                      io.cdap.cdap.etl.mock.batch.MockSink.class,
                      io.cdap.cdap.etl.mock.spark.streaming.MockSink.class,
                      DoubleTransform.class, AllErrorTransform.class, IdentityTransform.class,
                      IntValueFilterTransform.class, StringValueFilterTransform.class,
                      StringValueFilterCompute.class, Window.class,
                      NullFieldSplitterTransform.class, NullAlertTransform.class);
  }

  protected static void waitForAppToDeploy(ApplicationManager appManager, ApplicationId pipeline) throws Exception {
    Retries.callWithRetries(() -> {
      try {
        appManager.getInfo();
        return null;
      } catch (ApplicationNotFoundException exception) {
        throw new RetryableException(String.format("App %s not yet deployed", pipeline));
      }
    }, RetryStrategies.limit(10, RetryStrategies.fixDelay(15, TimeUnit.SECONDS)));
  }

}

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

package co.cask.cdap.etl.mock.test;

import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.mock.action.MockAction;
import co.cask.cdap.etl.mock.batch.MockExternalSink;
import co.cask.cdap.etl.mock.batch.MockExternalSource;
import co.cask.cdap.etl.mock.batch.MockRuntimeDatasetSink;
import co.cask.cdap.etl.mock.batch.MockRuntimeDatasetSource;
import co.cask.cdap.etl.mock.batch.NodeStatesAction;
import co.cask.cdap.etl.mock.batch.aggregator.FieldCountAggregator;
import co.cask.cdap.etl.mock.batch.aggregator.IdentityAggregator;
import co.cask.cdap.etl.mock.batch.joiner.MockJoiner;
import co.cask.cdap.etl.mock.realtime.LookupSource;
import co.cask.cdap.etl.mock.realtime.MockSink;
import co.cask.cdap.etl.mock.realtime.MockSource;
import co.cask.cdap.etl.mock.transform.DoubleTransform;
import co.cask.cdap.etl.mock.transform.ErrorTransform;
import co.cask.cdap.etl.mock.transform.FieldsPrefixTransform;
import co.cask.cdap.etl.mock.transform.IdentityTransform;
import co.cask.cdap.etl.mock.transform.IntValueFilterTransform;
import co.cask.cdap.etl.mock.transform.StringValueFilterTransform;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

/**
 * Performs common setup logic
 */
public class HydratorTestBase extends TestBase {
  private static final Set<PluginClass> REALTIME_MOCK_PLUGINS = ImmutableSet.of(
    LookupSource.PLUGIN_CLASS, MockSink.PLUGIN_CLASS, MockSource.PLUGIN_CLASS,
    DoubleTransform.PLUGIN_CLASS, ErrorTransform.PLUGIN_CLASS, IdentityTransform.PLUGIN_CLASS,
    IntValueFilterTransform.PLUGIN_CLASS, StringValueFilterTransform.PLUGIN_CLASS
  );
  private static final Set<PluginClass> BATCH_MOCK_PLUGINS = ImmutableSet.of(
    FieldCountAggregator.PLUGIN_CLASS, IdentityAggregator.PLUGIN_CLASS, MockJoiner.PLUGIN_CLASS,
    co.cask.cdap.etl.mock.batch.MockSink.PLUGIN_CLASS, co.cask.cdap.etl.mock.batch.MockSource.PLUGIN_CLASS,
    MockRuntimeDatasetSink.PLUGIN_CLASS, MockRuntimeDatasetSource.PLUGIN_CLASS,
    MockExternalSource.PLUGIN_CLASS, MockExternalSink.PLUGIN_CLASS,
    DoubleTransform.PLUGIN_CLASS, ErrorTransform.PLUGIN_CLASS, IdentityTransform.PLUGIN_CLASS,
    FieldsPrefixTransform.PLUGIN_CLASS, IntValueFilterTransform.PLUGIN_CLASS, StringValueFilterTransform.PLUGIN_CLASS,
    MockAction.PLUGIN_CLASS
  );

  public HydratorTestBase() {
  }

  protected static void setupRealtimeArtifacts(ArtifactId artifactId, Class<?> appClass) throws Exception {
    addAppArtifact(artifactId, appClass,
                   RealtimeSource.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName());

    // need to specify each PluginClass so that they can be used outside of this project. If we don't do this,
    // when the plugin jar is created, it will add lib/hydrator-test.jar to the plugin jar.
    // The ArtifactInspector will not examine any library jars for plugins, because it assumes plugins are always
    // .class files in the jar and never in the dependencies, which is normally a reasonable assumption.
    // So since the plugins are in lib/hydrator-test.jar, CDAP won't find any plugins in the jar.
    // To work around, we'll just explicitly specify each plugin.
    Set<PluginClass> pluginClasses = new HashSet<>();

    addPluginArtifact(new ArtifactId(artifactId.getNamespace(), artifactId.getArtifact() + "-mocks", "1.0.0"),
                      artifactId,
                      REALTIME_MOCK_PLUGINS,
                      MockSink.class, MockSource.class, LookupSource.class,
                      DoubleTransform.class, ErrorTransform.class, IdentityTransform.class,
                      IntValueFilterTransform.class, StringValueFilterTransform.class);
  }

  protected static void setupBatchArtifacts(ArtifactId artifactId, Class<?> appClass) throws Exception {
    // add the app artifact
    addAppArtifact(artifactId, appClass,
                   BatchSource.class.getPackage().getName(),
                   Action.class.getPackage().getName(),
                   PipelineConfigurable.class.getPackage().getName(),
                   "org.apache.avro.mapred", "org.apache.avro", "org.apache.avro.generic", "org.apache.avro.io");

    // add plugins artifact
    addPluginArtifact(new ArtifactId(artifactId.getNamespace(), artifactId.getArtifact() + "-mocks", "1.0.0"),
                      artifactId,
                      BATCH_MOCK_PLUGINS,
                      co.cask.cdap.etl.mock.batch.MockSource.class,
                      co.cask.cdap.etl.mock.batch.MockSink.class,
                      MockExternalSource.class, MockExternalSink.class,
                      DoubleTransform.class, ErrorTransform.class, IdentityTransform.class,
                      IntValueFilterTransform.class, StringValueFilterTransform.class,
                      FieldCountAggregator.class, IdentityAggregator.class, FieldsPrefixTransform.class,
                      NodeStatesAction.class);
  }

}

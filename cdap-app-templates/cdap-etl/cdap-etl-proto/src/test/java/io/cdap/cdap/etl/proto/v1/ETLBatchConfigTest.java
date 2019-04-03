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

package co.cask.cdap.etl.proto.v1;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.proto.ArtifactSelectorConfig;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.proto.UpgradeContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 */
public class ETLBatchConfigTest {

  @Test
  public void testUpgrade() throws Exception {
    final ArtifactSelectorConfig artifact = new ArtifactSelectorConfig("SYSTEM", "universal", "1.0.0");
    ETLStage source = new ETLStage(
      "source", new Plugin("DataGenerator", ImmutableMap.of("p1", "v1"), artifact), null);
    co.cask.cdap.etl.proto.v2.ETLStage sourceNew = from(source, BatchSource.PLUGIN_TYPE);

    ETLStage transform1 = new ETLStage(
      "transform1", new Plugin("Script", ImmutableMap.of("script", "something"), null));
    co.cask.cdap.etl.proto.v2.ETLStage transform1New = from(transform1, Transform.PLUGIN_TYPE);

    ETLStage transform2 = new ETLStage("transform2", new Plugin("Script", null, null));
    co.cask.cdap.etl.proto.v2.ETLStage transform2New = from(transform2, Transform.PLUGIN_TYPE);

    ETLStage transform3 = new ETLStage("transform3",
                                       new Plugin("Validator", ImmutableMap.of("p1", "v1", "p2", "v2")), null);
    co.cask.cdap.etl.proto.v2.ETLStage transform3New = from(transform3, Transform.PLUGIN_TYPE);

    ETLStage sink1 = new ETLStage("sink1", new Plugin("Table", ImmutableMap.of("rowkey", "xyz"), artifact), null);
    co.cask.cdap.etl.proto.v2.ETLStage sink1New = from(sink1, BatchSink.PLUGIN_TYPE);

    ETLStage sink2 = new ETLStage("sink2", new Plugin("HDFS", ImmutableMap.of("name", "abc"), artifact), null);
    co.cask.cdap.etl.proto.v2.ETLStage sink2New = from(sink2, BatchSink.PLUGIN_TYPE);

    Set<Connection> connections = new HashSet<>();
    connections.add(new Connection(sourceNew.getName(), transform1New.getName()));
    connections.add(new Connection(transform1New.getName(), transform2New.getName()));
    connections.add(new Connection(transform2New.getName(), transform3New.getName()));
    connections.add(new Connection(transform3New.getName(), sink1New.getName()));
    connections.add(new Connection(transform3New.getName(), sink2New.getName()));

    String schedule = "*/5 * * * *";
    Resources resources = new Resources(1024, 1);
    ETLBatchConfig config = ETLBatchConfig.builder(schedule)
      .setSource(source)
      .addSink(sink1)
      .addSink(sink2)
      .addTransform(transform1)
      .addTransform(transform2)
      .addTransform(transform3)
      .addConnections(connections)
      .setResources(resources)
      .setDriverResources(resources)
      .build();

    co.cask.cdap.etl.proto.v2.ETLBatchConfig configNew = co.cask.cdap.etl.proto.v2.ETLBatchConfig.builder(schedule)
      .addStage(sourceNew)
      .addStage(sink1New)
      .addStage(sink2New)
      .addStage(transform1New)
      .addStage(transform2New)
      .addStage(transform3New)
      .addConnections(connections)
      .setResources(resources)
      .setDriverResources(resources)
      .build();

    Assert.assertEquals(configNew, config.upgrade(new UpgradeContext() {
      @Nullable
      @Override
      public ArtifactSelectorConfig getPluginArtifact(String pluginType, String pluginName) {
        return null;
      }
    }));
  }

  private co.cask.cdap.etl.proto.v2.ETLStage from(ETLStage stage, String pluginType) {
    return new co.cask.cdap.etl.proto.v2.ETLStage(
      stage.getName(),
      new ETLPlugin(stage.getPlugin().getName(), pluginType,
                    stage.getPlugin().getProperties(), stage.getPlugin().getArtifact()));
  }

}

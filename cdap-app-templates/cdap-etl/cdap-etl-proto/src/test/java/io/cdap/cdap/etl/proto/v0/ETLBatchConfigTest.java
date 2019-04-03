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

package co.cask.cdap.etl.proto.v0;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.etl.proto.ArtifactSelectorConfig;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.proto.UpgradeContext;
import co.cask.cdap.etl.proto.v1.Plugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 */
public class ETLBatchConfigTest {

  @Test
  public void testUpgrade() throws Exception {
    final ArtifactSelectorConfig artifact = new ArtifactSelectorConfig("SYSTEM", "universal", "1.0.0");
    ETLStage source = new ETLStage("DataGenerator", ImmutableMap.of("p1", "v1"), null);
    co.cask.cdap.etl.proto.v1.ETLStage sourceNew =
      new co.cask.cdap.etl.proto.v1.ETLStage("DataGenerator.1",
                                             new Plugin(source.getName(), source.getProperties(), artifact),
                                             source.getErrorDatasetName());

    ETLStage transform1 = new ETLStage("Script", ImmutableMap.of("script", "something"), null);
    co.cask.cdap.etl.proto.v1.ETLStage transform1New =
      new co.cask.cdap.etl.proto.v1.ETLStage("Script.2",
                                             new Plugin(transform1.getName(), transform1.getProperties(), artifact),
                                             transform1.getErrorDatasetName());

    ETLStage transform2 = new ETLStage("Script", null, null);
    co.cask.cdap.etl.proto.v1.ETLStage transform2New =
      new co.cask.cdap.etl.proto.v1.ETLStage("Script.3",
                                             new Plugin(transform2.getName(), transform2.getProperties(), artifact),
                                             transform2.getErrorDatasetName());

    ETLStage transform3 = new ETLStage("Validator", ImmutableMap.of("p1", "v1", "p2", "v2"), "errorDS");
    co.cask.cdap.etl.proto.v1.ETLStage transform3New =
      new co.cask.cdap.etl.proto.v1.ETLStage("Validator.4",
                                             new Plugin(transform3.getName(), transform3.getProperties(), artifact),
                                             transform3.getErrorDatasetName());

    ETLStage sink1 = new ETLStage("Table", ImmutableMap.of("rowkey", "xyz"), null);
    co.cask.cdap.etl.proto.v1.ETLStage sink1New =
      new co.cask.cdap.etl.proto.v1.ETLStage("Table.5",
                                             new Plugin(sink1.getName(), sink1.getProperties(), artifact),
                                             sink1.getErrorDatasetName());

    ETLStage sink2 = new ETLStage("HDFS", ImmutableMap.of("name", "abc"), null);
    co.cask.cdap.etl.proto.v1.ETLStage sink2New =
      new co.cask.cdap.etl.proto.v1.ETLStage("HDFS.6",
                                             new Plugin(sink2.getName(), sink2.getProperties(), artifact),
                                             sink2.getErrorDatasetName());

    ETLStage action = new ETLStage("Email", ImmutableMap.of("email", "slj@example.com"), null);
    co.cask.cdap.etl.proto.v1.ETLStage actionNew =
      new co.cask.cdap.etl.proto.v1.ETLStage("Email.1",
                                             new Plugin(action.getName(), action.getProperties(), artifact),
                                             action.getErrorDatasetName());

    List<Connection> connections = new ArrayList<>();
    connections.add(new Connection(sourceNew.getName(), transform1New.getName()));
    connections.add(new Connection(transform1New.getName(), transform2New.getName()));
    connections.add(new Connection(transform2New.getName(), transform3New.getName()));
    connections.add(new Connection(transform3New.getName(), sink1New.getName()));
    connections.add(new Connection(transform3New.getName(), sink2New.getName()));

    String schedule = "*/5 * * * *";
    Resources resources = new Resources(1024, 1);
    ETLBatchConfig config = new ETLBatchConfig(schedule,
                                               source,
                                               ImmutableList.of(sink1, sink2),
                                               ImmutableList.of(transform1, transform2, transform3),
                                               resources,
                                               ImmutableList.of(action));
    co.cask.cdap.etl.proto.v1.ETLBatchConfig configNew = co.cask.cdap.etl.proto.v1.ETLBatchConfig.builder(schedule)
      .setSource(sourceNew)
      .addSink(sink1New)
      .addSink(sink2New)
      .addTransform(transform1New)
      .addTransform(transform2New)
      .addTransform(transform3New)
      .addConnections(connections)
      .setResources(resources)
      .setDriverResources(resources)
      .addAction(actionNew)
      .build();
    Assert.assertEquals(configNew, config.upgrade(new UpgradeContext() {
      @Nullable
      @Override
      public ArtifactSelectorConfig getPluginArtifact(String pluginType, String pluginName) {
        return new ArtifactSelectorConfig(ArtifactScope.SYSTEM.name(), "universal", "1.0.0");
      }
    }));
  }
}

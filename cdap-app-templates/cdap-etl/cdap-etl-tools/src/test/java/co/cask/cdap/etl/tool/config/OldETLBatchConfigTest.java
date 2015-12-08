/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.tool.config;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.Connection;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class OldETLBatchConfigTest {

  @Test
  public void testGetNewConfig() {
    OldETLStage source = new OldETLStage("DataGenerator", ImmutableMap.of("p1", "v1"), null);
    ETLStage sourceNew = new ETLStage("DataGenerator.1",
                                      new Plugin(source.getName(), source.getProperties()),
                                      source.getErrorDatasetName());

    OldETLStage transform1 = new OldETLStage("Script", ImmutableMap.of("script", "something"), null);
    ETLStage transform1New = new ETLStage("Script.2",
                                          new Plugin(transform1.getName(), transform1.getProperties()),
                                          transform1.getErrorDatasetName());

    OldETLStage transform2 = new OldETLStage("Script", null, null);
    ETLStage transform2New = new ETLStage("Script.3",
                                          new Plugin(transform2.getName(), transform2.getProperties()),
                                          transform2.getErrorDatasetName());

    OldETLStage transform3 = new OldETLStage("Validator", ImmutableMap.of("p1", "v1", "p2", "v2"), "errorDS");
    ETLStage transform3New = new ETLStage("Validator.4",
                                          new Plugin(transform3.getName(), transform3.getProperties()),
                                          transform3.getErrorDatasetName());

    OldETLStage sink1 = new OldETLStage("Table", ImmutableMap.of("rowkey", "xyz"), null);
    ETLStage sink1New = new ETLStage("Table.5",
                                     new Plugin(sink1.getName(), sink1.getProperties()),
                                     sink1.getErrorDatasetName());

    OldETLStage sink2 = new OldETLStage("HDFS", ImmutableMap.of("name", "abc"), null);
    ETLStage sink2New = new ETLStage("HDFS.6",
                                     new Plugin(sink2.getName(), sink2.getProperties()),
                                     sink2.getErrorDatasetName());

    OldETLStage action = new OldETLStage("Email", ImmutableMap.of("email", "slj@example.com"), null);
    ETLStage actionNew = new ETLStage("Email.1",
                                      new Plugin(action.getName(), action.getProperties()),
                                      action.getErrorDatasetName());

    List<Connection> connections = new ArrayList<>();
    connections.add(new Connection(sourceNew.getName(), transform1New.getName()));
    connections.add(new Connection(transform1New.getName(), transform2New.getName()));
    connections.add(new Connection(transform2New.getName(), transform3New.getName()));
    connections.add(new Connection(transform3New.getName(), sink1New.getName()));
    connections.add(new Connection(transform3New.getName(), sink2New.getName()));

    String schedule = "*/5 * * * *";
    Resources resources = new Resources(1024, 1);
    OldETLBatchConfig config = new OldETLBatchConfig(schedule,
                                                     source,
                                                     ImmutableList.of(sink1, sink2),
                                                     ImmutableList.of(transform1, transform2, transform3),
                                                     resources,
                                                     ImmutableList.of(action));
    ETLBatchConfig configNew = new ETLBatchConfig(schedule,
                                                  sourceNew,
                                                  ImmutableList.of(sink1New, sink2New),
                                                  ImmutableList.of(transform1New, transform2New, transform3New),
                                                  connections,
                                                  resources,
                                                  ImmutableList.of(actionNew));
    Assert.assertEquals(configNew, config.getNewConfig());
  }
}

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

package io.cdap.cdap.etl.mock.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.api.workflow.WorkflowNodeState;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Ending action writes the workflow's node states to a Table.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(NodeStatesAction.NAME)
public class NodeStatesAction extends PostAction {

  public static final String NAME = "TokenWriter";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();

  private final Conf conf;

  public NodeStatesAction(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(conf.tableName, Table.class);
  }

  @Override
  public void run(BatchActionContext context) throws Exception {
    Table table = context.getDataset(conf.tableName);
    for (Map.Entry<String, WorkflowNodeState> entry : context.getNodeStates().entrySet()) {
      Put put = new Put(entry.getKey());
      WorkflowNodeState nodeState = entry.getValue();
      put.add("runid", nodeState.getRunId());
      put.add("nodeid", nodeState.getNodeId());
      put.add("status", nodeState.getNodeStatus().name());
      table.put(put);
    }
  }

  public static ETLPlugin getPlugin(String tableName) {
    return new ETLPlugin("TokenWriter", PostAction.PLUGIN_TYPE, ImmutableMap.of("tableName", tableName), null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("tableName", new PluginPropertyField("tableName", "", "string", true, false));

    return PluginClass.builder()
      .setName(NodeStatesAction.NAME)
      .setType(PostAction.PLUGIN_TYPE)
      .setDescription("")
      .setClassName(NodeStatesAction.class.getName())
      .setProperties(properties)
      .setConfigFieldName("conf")
      .build();
  }

  /**
   * Conf for the token writer.
   */
  public static class Conf extends PluginConfig {
    private String tableName;
  }
}

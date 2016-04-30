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

package co.cask.cdap.etl.mock.batch;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.workflow.WorkflowNodeState;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchActionContext;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Ending action writes the workflow's node states to a Table.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name("TokenWriter")
public class NodeStatesAction extends PostAction {
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

  /**
   * Conf for the token writer.
   */
  public static class Conf extends PluginConfig {
    private String tableName;
  }
}

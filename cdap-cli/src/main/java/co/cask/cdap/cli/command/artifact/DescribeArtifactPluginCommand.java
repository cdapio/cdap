/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.cli.command.artifact;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.proto.artifact.PluginInfo;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists all plugins of a specific type available to an artifact.
 */
public class DescribeArtifactPluginCommand extends AbstractAuthCommand {
  private static final Gson GSON = new Gson();

  private final ArtifactClient artifactClient;

  @Inject
  public DescribeArtifactPluginCommand(ArtifactClient artifactClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.artifactClient = artifactClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {

    String artifactName = arguments.get(ArgumentName.ARTIFACT_NAME.toString());
    String artifactVersion = arguments.get(ArgumentName.ARTIFACT_VERSION.toString());
    ArtifactId artifactId = cliConfig.getCurrentNamespace().artifact(artifactName, artifactVersion);
    String pluginType = arguments.get(ArgumentName.PLUGIN_TYPE.toString());
    String pluginName = arguments.get(ArgumentName.PLUGIN_NAME.toString());

    List<PluginInfo> pluginInfos;
    String scopeStr = arguments.getOptional(ArgumentName.SCOPE.toString());
    if (scopeStr == null) {
      pluginInfos = artifactClient.getPluginInfo(artifactId.toId(), pluginType, pluginName);
    } else {
      pluginInfos = artifactClient.getPluginInfo(artifactId.toId(), pluginType, pluginName,
        ArtifactScope.valueOf(scopeStr.toUpperCase()));
    }
    Table table = Table.builder()
      .setHeader("type", "name", "classname", "description", "properties", "artifact")
      .setRows(pluginInfos, new RowMaker<PluginInfo>() {
        @Override
        public List<?> makeRow(PluginInfo object) {
          return Lists.newArrayList(
            object.getType(), object.getName(), object.getClassName(), object.getDescription(),
            GSON.toJson(object.getProperties()), object.getArtifact().toString());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("describe artifact-plugin <%s> <%s> <%s> <%s> [<%s>]", ArgumentName.ARTIFACT_NAME,
      ArgumentName.ARTIFACT_VERSION, ArgumentName.PLUGIN_TYPE, ArgumentName.PLUGIN_NAME, ArgumentName.SCOPE);
  }

  @Override
  public String getDescription() {
    return String.format("Describes a plugin of a specific type and name available to a specific %s. " +
                         "Can return multiple details if there are multiple versions of the plugin. If no scope is " +
                         "provided, plugins are looked for first in the SYSTEM and then in the USER scope.",
                         ElementType.ARTIFACT.getName());
  }
}

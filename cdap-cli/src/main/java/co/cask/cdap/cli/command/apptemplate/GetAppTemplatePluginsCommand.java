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

package co.cask.cdap.cli.command.apptemplate;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.ApplicationTemplateClient;
import co.cask.cdap.proto.template.PluginMeta;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Gets the plugins for an application template.
 */
public class GetAppTemplatePluginsCommand extends AbstractAuthCommand {

  private static final Gson GSON = new Gson();
  private final ApplicationTemplateClient client;

  @Inject
  public GetAppTemplatePluginsCommand(ApplicationTemplateClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String adapterName = arguments.get(ArgumentName.APP_TEMPLATE.toString());
    String pluginType = arguments.get(ArgumentName.PLUGIN_TYPE.toString());
    List<PluginMeta> list = client.getPlugins(adapterName, pluginType);

    Table table = Table.builder()
      .setHeader("name", "description", "type", "source")
      .setRows(list, new RowMaker<PluginMeta>() {
        @Override
        public List<?> makeRow(PluginMeta object) {
          return Lists.newArrayList(object.getName(), object.getDescription(), object.getType(),
                                    GSON.toJson(object.getSource()));
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("get app-template plugins <%s> <%s>", ArgumentName.APP_TEMPLATE, ArgumentName.PLUGIN_TYPE);
  }

  @Override
  public String getDescription() {
    return String.format("Lists plugins for %s.", Fragment.of(Article.A, ElementType.APP_TEMPLATE.getName()));
  }
}

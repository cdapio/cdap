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

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.ApplicationTemplateClient;
import co.cask.cdap.proto.template.ApplicationTemplateMeta;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Lists application templates.
 */
public class ListAppTemplatesCommand extends AbstractAuthCommand {

  private final ApplicationTemplateClient client;

  @Inject
  public ListAppTemplatesCommand(ApplicationTemplateClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    List<ApplicationTemplateMeta> list = client.list();

    Table table = Table.builder()
      .setHeader("name", "description", "program type")
      .setRows(list, new RowMaker<ApplicationTemplateMeta>() {
        @Override
        public List<?> makeRow(ApplicationTemplateMeta object) {
          return Lists.newArrayList(object.getName(), object.getDescription(), object.getProgramType());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return "list app-templates";
  }

  @Override
  public String getDescription() {
    return String.format("Lists all %s.", ElementType.APP_TEMPLATE.getTitleNamePlural());
  }
}

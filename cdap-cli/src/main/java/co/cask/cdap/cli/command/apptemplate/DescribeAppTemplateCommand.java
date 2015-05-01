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
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.ApplicationTemplateClient;
import co.cask.cdap.proto.template.ApplicationTemplateDetail;
import co.cask.common.cli.Arguments;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

/**
 * Describes an application template.
 */
public class DescribeAppTemplateCommand extends AbstractAuthCommand {

  private final ApplicationTemplateClient client;

  @Inject
  public DescribeAppTemplateCommand(ApplicationTemplateClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String adapterName = arguments.get(ArgumentName.APP_TEMPLATE.toString());
    ApplicationTemplateDetail spec = client.get(adapterName);

    Table table = Table.builder()
      .setHeader("name", "description", "program type", "plugin types")
      .setRows(Collections.singletonList(spec), new RowMaker<ApplicationTemplateDetail>() {
        @Override
        public List<?> makeRow(ApplicationTemplateDetail object) {
          return Lists.newArrayList(object.getName(), object.getDescription(), object.getProgramType(),
                                    Joiner.on(",").join(object.getExtensions()));
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("describe app-template <%s>", ArgumentName.APP_TEMPLATE);
  }

  @Override
  public String getDescription() {
    return String.format("Lists all %s.", ElementType.APP_TEMPLATE.getTitleNamePlural());
  }
}

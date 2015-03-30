/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.command.system;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.table.AltStyleTableRenderer;
import co.cask.cdap.cli.util.table.CsvTableRenderer;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * Changes how the CLI renders tables.
 */
public class RenderAsCommand implements Command {

  /**
   * Type of table renderer.
   */
  public enum Type {
    CSV(new CsvTableRenderer()), ALT(new AltStyleTableRenderer());

    private final TableRenderer tableRenderer;

    Type(TableRenderer tableRenderer) {
      this.tableRenderer = tableRenderer;
    }

    public TableRenderer getTableRenderer() {
      return tableRenderer;
    }
  }

  private final CLIConfig cliConfig;

  @Inject
  public RenderAsCommand(CLIConfig cliConfig) {
    this.cliConfig = cliConfig;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    Type type = Type.valueOf(arguments.get(ArgumentName.TABLE_RENDERER.getName()).toUpperCase());
    cliConfig.setTableRenderer(type.getTableRenderer());
    output.println("Now rendering as " + type.name());
  }

  @Override
  public String getPattern() {
    return String.format("cli render as <%s>", ArgumentName.TABLE_RENDERER);
  }

  @Override
  public String getDescription() {
    return "Modifies how table data is rendered. Valid options are \"alt\" (default) and \"csv\".";
  }
}

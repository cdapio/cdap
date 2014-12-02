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

package co.cask.cdap.cli.docs;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.Constants;
import co.cask.cdap.cli.DefaultCommands;
import co.cask.cdap.cli.command.HelpCommand;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Generates data for the table in cdap-docs/reference-manual/source/cli-api.rst.
 *
 */
public class GenerateCLIDocsTable {

  private final Command printDocsCommand;

  private final Iterable<Command> commands;

  public GenerateCLIDocsTable(final CLIConfig cliConfig) throws URISyntaxException, IOException {
    this.printDocsCommand = new Command() {

      @Override
      public void execute(Arguments arguments, PrintStream output) throws Exception {
        List<Command> commandList = com.googlecode.concurrenttrees.common.Iterables.toList(commands);
        Collections.sort(commandList, new Comparator<Command>() {
          @Override
          public int compare(Command command, Command command2) {
            return command.getPattern().compareTo(command2.getPattern());
          }
        });
        for (Command command : commandList) {
          String description = command.getDescription().replace("\"", "\"\"");
          output.printf("   ``%s``,\"%s\"\n", command.getPattern(), description);
        }
      }

      @Override
      public String getPattern() {
        return "null";
      }

      @Override
      public String getDescription() {
        return "null";
      }
    };

    Injector injector = Guice.createInjector(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(CLIConfig.class).toInstance(cliConfig);
          bind(ClientConfig.class).toInstance(cliConfig.getClientConfig());
          bind(CConfiguration.class).toInstance(CConfiguration.create());
        }
      }
    );

    this.commands = Iterables.concat(new DefaultCommands(injector).get(),
                                     ImmutableList.<Command>of(new HelpCommand(null, null)));
  }

  public static void main(String[] args) throws Exception {
    String hostname = Objects.firstNonNull(System.getenv(Constants.EV_HOSTNAME), "localhost");
    PrintStream output = System.out;

    CLIConfig config = new CLIConfig(hostname);
    GenerateCLIDocsTable generateCLIDocsTable = new GenerateCLIDocsTable(config);
    generateCLIDocsTable.printDocsCommand.execute(null, output);
  }
}

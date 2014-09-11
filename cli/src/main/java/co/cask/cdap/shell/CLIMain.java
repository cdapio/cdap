/*
 * Copyright 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.shell;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.shell.command.CallCommandSet;
import co.cask.cdap.shell.command.ConnectCommand;
import co.cask.cdap.shell.command.CreateCommandSet;
import co.cask.cdap.shell.command.DeleteCommandSet;
import co.cask.cdap.shell.command.DeployCommandSet;
import co.cask.cdap.shell.command.DescribeCommandSet;
import co.cask.cdap.shell.command.ExecuteQueryCommand;
import co.cask.cdap.shell.command.ExitCommand;
import co.cask.cdap.shell.command.GetCommandSet;
import co.cask.cdap.shell.command.HelpCommand;
import co.cask.cdap.shell.command.ListCommandSet;
import co.cask.cdap.shell.command.SendCommandSet;
import co.cask.cdap.shell.command.SetCommandSet;
import co.cask.cdap.shell.command.StartProgramCommandSet;
import co.cask.cdap.shell.command.StopProgramCommandSet;
import co.cask.cdap.shell.command.TruncateCommandSet;
import co.cask.cdap.shell.command.VersionCommand;
import co.cask.cdap.shell.exception.InvalidCommandException;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.Completer;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;

/**
 * Main class for the CDAP CLI.
 */
public class CLIMain {

  private final CommandSet commands;
  private final CLIConfig cliConfig;
  private final HelpCommand helpCommand;
  private final ConsoleReader reader;

  public CLIMain(final CLIConfig cliConfig) throws URISyntaxException, IOException {
    this.reader = new ConsoleReader();
    this.cliConfig = cliConfig;
    this.cliConfig.addHostnameChangeListener(new CLIConfig.HostnameChangeListener() {
      @Override
      public void onHostnameChanged(String newHostname) {
        reader.setPrompt("cdap (" + cliConfig.getHost() + ":" + cliConfig.getClientConfig().getPort() + ")> ");
      }
    });
    this.helpCommand = new HelpCommand(new Supplier<CommandSet>() {
      @Override
      public CommandSet get() {
        return getCommands();
      }
    }, cliConfig);

    Injector injector = Guice.createInjector(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(CLIConfig.class).toInstance(cliConfig);
          bind(ClientConfig.class).toInstance(cliConfig.getClientConfig());
        }
      }
    );

    this.commands = CommandSet.builder(null)
      .addCommand(helpCommand)
      .addCommand(injector.getInstance(ConnectCommand.class))
      .addCommand(injector.getInstance(VersionCommand.class))
      .addCommand(injector.getInstance(ExitCommand.class))
      .addCommand(injector.getInstance(CallCommandSet.class))
      .addCommand(injector.getInstance(CreateCommandSet.class))
      .addCommand(injector.getInstance(DeleteCommandSet.class))
      .addCommand(injector.getInstance(DeployCommandSet.class))
      .addCommand(injector.getInstance(DescribeCommandSet.class))
      .addCommand(injector.getInstance(ExecuteQueryCommand.class))
      .addCommand(injector.getInstance(GetCommandSet.class))
      .addCommand(injector.getInstance(ListCommandSet.class))
      .addCommand(injector.getInstance(SendCommandSet.class))
      .addCommand(injector.getInstance(SetCommandSet.class))
      .addCommand(injector.getInstance(StartProgramCommandSet.class))
      .addCommand(injector.getInstance(StopProgramCommandSet.class))
      .addCommand(injector.getInstance(TruncateCommandSet.class))
      .build();
  }

  /**
   * Starts shell mode, which provides a shell to enter multiple commands and use autocompletion.
   *
   * @param output {@link PrintStream} to write to
   * @throws Exception
   */
  public void startShellMode(PrintStream output) throws Exception {
    this.reader.setPrompt("cdap (" + cliConfig.getHost() + ":" + cliConfig.getClientConfig().getPort() + ")> ");
    this.reader.setHandleUserInterrupt(true);

    for (Completer completer : commands.getCompleters(null)) {
      reader.addCompleter(completer);
    }

    while (true) {
      String line;

      try {
        line = reader.readLine();
      } catch (UserInterruptException e) {
        continue;
      }

      if (line == null) {
        output.println();
        break;
      }

      if (line.length() > 0) {
        String command = line.trim();
        String[] commandArgs = Iterables.toArray(Splitter.on(" ").split(command), String.class);
        try {
          processArgs(commandArgs, output);
        } catch (InvalidCommandException e) {
          output.println("Invalid command: " + command + " (enter 'help' to list all available commands)");
        } catch (Exception e) {
          output.println("Error: " + e.getMessage());
        }
        output.println();
      }
    }
  }

  /**
   * Processes a command and writes to the provided output
   * @param args the tokens of the command string (e.g. ["start", "flow", "SomeApp.SomeFlow"])
   * @throws Exception
   */
  public void processArgs(String[] args, PrintStream output) throws Exception {
    commands.process(args, output);
  }

  private CommandSet getCommands() {
    return commands;
  }

  public static void main(String[] args) throws Exception {
    String hostname = Objects.firstNonNull(System.getenv(Constants.EV_HOSTNAME), "localhost");

    CLIConfig config = new CLIConfig(hostname);
    CLIMain shell = new CLIMain(config);

    if (args.length == 0) {
      shell.startShellMode(System.out);
    } else {
      shell.processArgs(args, System.out);
    }
  }
}

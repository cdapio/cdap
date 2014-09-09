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
import co.cask.cdap.shell.command.Arguments;
import co.cask.cdap.shell.command.Command;
import co.cask.cdap.shell.command.CommandMatch;
import co.cask.cdap.shell.command.CommandSet;
import co.cask.cdap.shell.command.CompleterSet;
import co.cask.cdap.shell.command.DefaultCommands;
import co.cask.cdap.shell.command.DefaultCompleters;
import co.cask.cdap.shell.command.common.HelpCommand;
import co.cask.cdap.shell.completer.DefaultStringsCompleter;
import co.cask.cdap.shell.completer.PrefixCompleter;
import co.cask.cdap.shell.exception.InvalidCommandException;
import co.cask.cdap.shell.util.TreeNode;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Main class for the CDAP CLI.
 */
public class CLIMain {

  private final CLIConfig cliConfig;
  private final HelpCommand helpCommand;
  private final ConsoleReader reader;

  private final CommandSet commands;
  private final CompleterSet completers;

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

    this.completers = new DefaultCompleters(injector);
    this.commands = new DefaultCommands(injector);
  }

  public CommandSet getCommands() {
    return commands;
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

    TreeNode<String> commandTokenTree = new TreeNode<String>();
    for (Command command : commands.getCommands()) {
      String pattern = command.getPattern();
      String[] tokens = pattern.split(" ");

      TreeNode<String> currentNode = commandTokenTree;
      for (String token : tokens) {
        currentNode = currentNode.findOrCreateChild(token);
      }
    }

    List<Completer> completerList = generateCompleters(null, commandTokenTree);
    for (Completer completer : completerList) {
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
        try {
          processArgs(command, output);
        } catch (InvalidCommandException e) {
          output.println("Invalid command: " + command + " (enter 'help' to list all available commands)");
        } catch (Exception e) {
          output.println("Error: " + e.getMessage());
        }
        output.println();
      }
    }
  }

  private List<Completer> generateCompleters(String prefix, TreeNode<String> commandTokenTree) {
    List<Completer> completers = Lists.newArrayList();
    String name = commandTokenTree.getData();
    String childPrefix = (prefix == null || prefix.isEmpty() ? "" : prefix + " ") + (name == null ? "" : name);

    if (!commandTokenTree.getChildren().isEmpty()) {
      List<String> nonArgumentTokens = Lists.newArrayList();
      List<String> argumentTokens = Lists.newArrayList();
      for (TreeNode<String> child : commandTokenTree.getChildren()) {
        String childToken = child.getData();
        if (childToken.matches("<\\S+>")) {
          argumentTokens.add(childToken);
        } else {
          nonArgumentTokens.add(child.getData());
        }
      }

      for (String argumentToken : argumentTokens) {
        // chop off the < and > or [ and ]
        String completerType = argumentToken.substring(1, argumentToken.length() - 1);
        Completer argumentCompleter = getCompleterForType(completerType);
        if (argumentCompleter != null) {
          completers.add(prefixCompleterIfNeeded(childPrefix, argumentCompleter));
        }
      }

      completers.add(prefixCompleterIfNeeded(childPrefix, new DefaultStringsCompleter(nonArgumentTokens)));

      for (TreeNode<String> child : commandTokenTree.getChildren()) {
        completers.addAll(generateCompleters(childPrefix, child));
      }
    }

    return Lists.<Completer>newArrayList(new AggregateCompleter(completers));
  }

  private Completer prefixCompleterIfNeeded(String prefix, Completer completer) {
    if (prefix != null && !prefix.isEmpty()) {
      return new PrefixCompleter(prefix.replaceAll("<\\S+>", "{}"), completer);
    } else {
      return completer;
    }
  }

  private Completer getCompleterForType(String completerType) {
    return completers.getCompleter(completerType);
  }

  /**
   * Processes a command and writes to the provided output
   * @param input the command string (e.g. "start flow SomeApp.SomeFlow")
   * @throws Exception
   */
  public void processArgs(String input, PrintStream output) throws Exception {
    CommandMatch commandMatch = commands.findMatch(input);
    if (commandMatch == null) {
      throw new InvalidCommandException();
    }

    Preconditions.checkNotNull(commandMatch);
    Preconditions.checkNotNull(commandMatch.getCommand());

    Command command = commandMatch.getCommand();
    Arguments arguments = commandMatch.getArguments();
    command.execute(arguments, output);
  }

  public static void main(String[] args) throws Exception {
    String hostname = Objects.firstNonNull(System.getenv(Constants.ENV_HOSTNAME), "localhost");

    CLIConfig config = new CLIConfig(hostname);
    CLIMain shell = new CLIMain(config);

    if (args.length == 0) {
      shell.startShellMode(System.out);
    } else {
      shell.processArgs(Joiner.on(" ").join(args), System.out);
    }
  }
}

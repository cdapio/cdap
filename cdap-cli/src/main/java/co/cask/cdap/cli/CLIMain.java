/*
 * Copyright © 2012-2015 Cask Data, Inc.
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

package co.cask.cdap.cli;

import co.cask.cdap.cli.command.ConnectCommand;
import co.cask.cdap.cli.command.HelpCommand;
import co.cask.cdap.cli.command.SearchCommandsCommand;
import co.cask.cdap.cli.commandset.DefaultCommands;
import co.cask.cdap.cli.completer.supplier.EndpointSupplier;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.common.cli.CLI;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import co.cask.common.cli.exception.CLIExceptionHandler;
import co.cask.common.cli.exception.InvalidCommandException;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import jline.console.completer.Completer;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import javax.net.ssl.SSLHandshakeException;

/**
 * Main class for the CDAP CLI.
 */
public class CLIMain {

  private final CLI cli;

  private final Iterable<CommandSet<Command>> commands;

  public CLIMain(final CLIConfig cliConfig) throws URISyntaxException, IOException {
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

    ConnectCommand connectCommand = injector.getInstance(ConnectCommand.class);
    if (!cliConfig.isHostnameProvided()) {
      connectCommand.tryDefaultConnection(System.out, false);
    }

    this.commands = ImmutableList.of(
      injector.getInstance(DefaultCommands.class),
      new CommandSet<Command>(ImmutableList.<Command>of(
        new HelpCommand(getCommandsSupplier()),
        new SearchCommandsCommand(getCommandsSupplier())
      )));

    Map<String, Completer> completers = injector.getInstance(DefaultCompleters.class).get();
    cli = new CLI<Command>(Iterables.concat(commands), completers);
    cli.getReader().setPrompt(cliConfig.getPrompt());
    cli.setExceptionHandler(new CLIExceptionHandler<Exception>() {
      @Override
      public boolean handleException(PrintStream output, Exception e, int timesRetried) {
        if (e instanceof SSLHandshakeException) {
          output.printf("To ignore this error, set -D%s=false when starting the CLI\n",
                        CLIConfig.PROP_VERIFY_SSL_CERT);
        } else if (e instanceof InvalidCommandException) {
          InvalidCommandException ex = (InvalidCommandException) e;
          output.printf("Invalid command '%s'. Enter 'help' for a list of commands\n", ex.getInput());
        } else {
          e.printStackTrace();
          output.println("Error: " + e.getMessage());
        }

        return false;
      }
    });
    cli.addCompleterSupplier(injector.getInstance(EndpointSupplier.class));

    cliConfig.addHostnameChangeListener(new CLIConfig.ConnectionChangeListener() {
      @Override
      public void onConnectionChanged(String newNamespace, URI newURI) {
        cli.getReader().setPrompt(cliConfig.getPrompt());
      }
    });
  }

  public CLI getCLI() {
    return this.cli;
  }

  public Supplier<Iterable<CommandSet<Command>>> getCommandsSupplier() {
    return new Supplier<Iterable<CommandSet<Command>>>() {
      @Override
      public Iterable<CommandSet<Command>> get() {
        return commands;
      }
    };
  }

  public static void main(String[] args) throws Exception {
    String hostname = System.getenv(Constants.EV_HOSTNAME);
    PrintStream output = System.out;

    CLIConfig config = new CLIConfig(hostname);
    CLIMain cliMain = new CLIMain(config);
    CLI cli = cliMain.getCLI();

    if (args.length == 0) {
      cli.startInteractiveMode(output);
    } else {
      cli.execute(Joiner.on(" ").join(args), output);
    }
  }
}

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

package co.cask.cdap.cli;

import co.cask.cdap.cli.command.ConnectCommand;
import co.cask.cdap.cli.command.HelpCommand;
import co.cask.cdap.cli.completer.supplier.EndpointSupplier;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.common.cli.CLI;
import co.cask.common.cli.Command;
import co.cask.common.cli.exception.CLIExceptionHandler;
import co.cask.common.cli.exception.InvalidCommandException;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
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

  private final CLIConfig cliConfig;
  private final HelpCommand helpCommand;
  private final CLI cli;

  private final Iterable<Command> commands;
  private final Map<String, Completer> completers;

  public CLIMain(final CLIConfig cliConfig) throws URISyntaxException, IOException {
    this.cliConfig = cliConfig;
    this.helpCommand = new HelpCommand(new Supplier<Iterable<Command>>() {
      @Override
      public Iterable<Command> get() {
        return getCommands();
      }
    }, cliConfig);

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
    connectCommand.tryDefaultConnection(System.out, false);

    this.commands = Iterables.concat(new DefaultCommands(injector).get(),
                                     ImmutableList.<Command>of(helpCommand));
    this.completers = new DefaultCompleters(injector).get();
    cli = new CLI<Command>(commands, completers);
    cli.getReader().setPrompt("cdap (" + cliConfig.getURI() + ")> ");
    cli.setExceptionHandler(new CLIExceptionHandler<Exception>() {
      @Override
      public void handleException(PrintStream output, Exception e) {
        if (e instanceof SSLHandshakeException) {
          output.printf("To ignore this error, set -D%s=false when starting the CLI\n",
                        CLIConfig.PROP_VERIFY_SSL_CERT);
        } else if (e instanceof InvalidCommandException) {
          InvalidCommandException ex = (InvalidCommandException) e;
          output.printf("Invalid command '%s'. Enter 'help' for a list of commands\n", ex.getInput());
        } else {
          output.println("Error: " + e.getMessage());
        }
      }
    });
    cli.addCompleterSupplier(injector.getInstance(EndpointSupplier.class));

    this.cliConfig.addHostnameChangeListener(new CLIConfig.ConnectionChangeListener() {
      @Override
      public void onConnectionChanged(URI newURI) {
        cli.getReader().setPrompt("cdap (" + newURI + ")> ");
      }
    });
  }

  public CLI getCLI() {
    return this.cli;
  }

  private Iterable<Command> getCommands() {
    return this.commands;
  }

  public static void main(String[] args) throws Exception {
    String hostname = Objects.firstNonNull(System.getenv(Constants.EV_HOSTNAME), "localhost");
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

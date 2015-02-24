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
import co.cask.cdap.common.conf.Constants;
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
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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

  private static final String DEFAULT_ACCESS_TOKEN_FILE = "~/.cdap.access.token";
  private static final boolean DEFAULT_VERIFY_SSL = true;
  private static final boolean DEFAULT_AUTOCONNECT = true;

  private static final Option URI_OPTION = new Option(
    "u", "uri", true, "URI of the CDAP instance to interact with in" +
    " the format \"[<http|https>://]<hostname>[:<port>[/<namespace>]]\"." +
    " Defaults to " + getDefaultURI() + ".");

  private static final Option VERIFY_SSL_OPTION = new Option(
    "v", "verify-ssl", true, "If true, verify SSL certificate when making requests." +
    " Defaults to " + DEFAULT_VERIFY_SSL + ".");

  private static final Option ACCESS_TOKEN_OPTION = new Option(
    "t", "access-token", true, "Path of the access token file to use when making requests." +
    " Defaults to " + DEFAULT_ACCESS_TOKEN_FILE + ".");

  private static final Option AUTOCONNECT_OPTION = new Option(
    "a", "autoconnect", true, "If true, automatically try default connection." +
    " Defaults to " + DEFAULT_AUTOCONNECT + ".");

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

    this.commands = ImmutableList.of(
      injector.getInstance(DefaultCommands.class),
      new CommandSet<Command>(ImmutableList.<Command>of(
        new HelpCommand(getCommandsSupplier()),
        new SearchCommandsCommand(getCommandsSupplier())
      )));

    Map<String, Completer> completers = injector.getInstance(DefaultCompleters.class).get();
    cli = new CLI<Command>(Iterables.concat(commands), completers);
    cli.setExceptionHandler(new CLIExceptionHandler<Exception>() {
      @Override
      public boolean handleException(PrintStream output, Exception e, int timesRetried) {
        if (e instanceof SSLHandshakeException) {
          output.println("SSL handshake failed. Try setting --" + VERIFY_SSL_OPTION.getLongOpt() +
                           "=false when starting the CLI.");
        } else if (e instanceof InvalidCommandException) {
          InvalidCommandException ex = (InvalidCommandException) e;
          output.printf("Invalid command '%s'. Enter 'help' for a list of commands.", ex.getInput());
          output.println();
        } else {
          output.println("Error: " + e.getMessage());
        }

        return false;
      }
    });
    cli.addCompleterSupplier(injector.getInstance(EndpointSupplier.class));

    setCLIPrompt(cliConfig.getCurrentNamespace(), cliConfig.getURI());
    cliConfig.addHostnameChangeListener(new CLIConfig.ConnectionChangeListener() {
      @Override
      public void onConnectionChanged(String newNamespace, URI newURI) {
        setCLIPrompt(newNamespace, newURI);
      }
    });
  }

  public static String getDefaultURI() {
    CConfiguration cConf = CConfiguration.create();
    boolean sslEnabled = cConf.getBoolean(Constants.Security.SSL_ENABLED);
    String hostname = cConf.get(Constants.Router.ADDRESS);
    int port = sslEnabled ?
      cConf.getInt(Constants.Router.ROUTER_SSL_PORT) :
      cConf.getInt(Constants.Router.ROUTER_PORT);
    String namespace = Constants.DEFAULT_NAMESPACE;

    return sslEnabled ? "https" : "http" + "://" + hostname + ":" + port + "/" + namespace;
  }

  private void setCLIPrompt(String namespace, URI uri) {
    cli.getReader().setPrompt("cdap (" + uri + "/" + namespace + ")> ");
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
    PrintStream output = System.out;

    Options options = getOptions();
    CommandLineParser parser = new BasicParser();
    try {
      CommandLine command = parser.parse(options, args);
      String uri = command.getOptionValue(URI_OPTION.getOpt(), getDefaultURI());
      String accessTokenFile = command.getOptionValue(ACCESS_TOKEN_OPTION.getOpt());
      boolean verifySSL = Boolean.parseBoolean(command.getOptionValue(VERIFY_SSL_OPTION.getOpt(), Boolean.toString(DEFAULT_VERIFY_SSL)));
      boolean autoconnect = Boolean.parseBoolean(command.getOptionValue(AUTOCONNECT_OPTION.getOpt(), Boolean.toString(DEFAULT_AUTOCONNECT)));
      String[] commandArgs = command.getArgs();

      CLILaunchConfig launchConfig = new CLILaunchConfig(uri, accessTokenFile, verifySSL, autoconnect);
      CLIConfig config = new CLIConfig(launchConfig);
      CLIMain cliMain = new CLIMain(config);
      CLI cli = cliMain.getCLI();

      if (commandArgs.length == 0) {
        cli.startInteractiveMode(output);
      } else {
        cli.execute(Joiner.on(" ").join(commandArgs), output);
      }
    } catch (ParseException e) {
      output.println(e.getMessage());
      usage();
    }
  }

  private static Options getOptions() {
    Options options = new Options();

    OptionGroup optionalGroup = new OptionGroup();
    optionalGroup.setRequired(false);
    optionalGroup.addOption(URI_OPTION);
    optionalGroup.addOption(VERIFY_SSL_OPTION);
    optionalGroup.addOption(ACCESS_TOKEN_OPTION);
    optionalGroup.addOption(AUTOCONNECT_OPTION);
    options.addOptionGroup(optionalGroup);

    return options;
  }

  private static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("CDAP CLI", getOptions());
    System.exit(0);
  }
}

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

import co.cask.cdap.cli.command.HelpCommand;
import co.cask.cdap.cli.command.SearchCommandsCommand;
import co.cask.cdap.cli.commandset.DefaultCommands;
import co.cask.cdap.cli.completer.supplier.EndpointSupplier;
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.cli.util.table.AltStyleTableRenderer;
import co.cask.cdap.cli.util.table.TableRenderer;
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

  private static final boolean DEFAULT_VERIFY_SSL = true;
  private static final boolean DEFAULT_AUTOCONNECT = true;

  private static final Option HELP_OPTION = new Option(
    "h", "help", false, "Print the usage message.");

  private static final Option URI_OPTION = new Option(
    "u", "uri", true, "CDAP instance URI to interact with in" +
    " the format \"[http[s]://]<hostname>[:<port>[/<namespace>]]\"." +
    " Defaults to \"" + getDefaultURI() + "\".");

  private static final Option VERIFY_SSL_OPTION = new Option(
    "s", "verify-ssl", true, "If \"true\", verify SSL certificate when making requests." +
    " Defaults to \"" + DEFAULT_VERIFY_SSL + "\".");

  private static final Option AUTOCONNECT_OPTION = new Option(
    "a", "autoconnect", true, "If \"true\", try provided connection" +
    " (from " + URI_OPTION.getLongOpt() + ")" +
    " upon launch or try default connection if none provided." +
    " Defaults to \"" + DEFAULT_AUTOCONNECT + "\".");

  private static final Option DEBUG_OPTION = new Option(
    "d", "debug", false, "Print exception stack traces.");

  private final CLI cli;
  private final Iterable<CommandSet<Command>> commands;
  private final LaunchOptions options;
  private final CLIConfig cliConfig;

  public CLIMain(final LaunchOptions options, final CLIConfig cliConfig) throws URISyntaxException, IOException {
    this.options = options;
    this.cliConfig = cliConfig;

    final PrintStream output = cliConfig.getOutput();
    final TableRenderer tableRenderer = cliConfig.getTableRenderer();

    cliConfig.getClientConfig().setVerifySSLCert(options.isVerifySSL());
    Injector injector = Guice.createInjector(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(LaunchOptions.class).toInstance(options);
          bind(CConfiguration.class).toInstance(CConfiguration.create());
          bind(PrintStream.class).toInstance(output);
          bind(CLIConfig.class).toInstance(cliConfig);
          bind(ClientConfig.class).toInstance(cliConfig.getClientConfig());
          bind(TableRenderer.class).toInstance(tableRenderer);
        }
      }
    );

    InstanceURIParser instanceURIParser = injector.getInstance(InstanceURIParser.class);
    if (options.isAutoconnect()) {
      try {
        CLIConfig.ConnectionInfo connectionInfo = instanceURIParser.parse(options.getUri());
        cliConfig.tryConnect(connectionInfo, output, options.isDebug());
        cliConfig.getClientConfig().setHostname(connectionInfo.getHostname());
        cliConfig.getClientConfig().setPort(connectionInfo.getPort());
        cliConfig.getClientConfig().setSSLEnabled(connectionInfo.isSSLEnabled());
        cliConfig.getClientConfig().setNamespace(connectionInfo.getNamespace());
      } catch (Exception e) {
        if (options.isDebug()) {
          e.printStackTrace(output);
        }
      }
    }

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
          output.printf("To ignore this error, set \"--%s false\" when starting the CLI\n",
                        VERIFY_SSL_OPTION.getLongOpt());
        } else if (e instanceof InvalidCommandException) {
          InvalidCommandException ex = (InvalidCommandException) e;
          output.printf("Invalid command '%s'. Enter 'help' for a list of commands\n", ex.getInput());
        } else {
          output.println("Error: " + e.getMessage());
        }

        if (options.isDebug()) {
          e.printStackTrace(output);
        }

        return false;
      }
    });
    cli.addCompleterSupplier(injector.getInstance(EndpointSupplier.class));

    updateCLIPrompt(cliConfig.getClientConfig());
    cliConfig.addHostnameChangeListener(new CLIConfig.ConnectionChangeListener() {
      @Override
      public void onConnectionChanged(ClientConfig clientConfig) {
        updateCLIPrompt(clientConfig);
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

  private String limit(String string, int maxLength) {
    if (string.length() <= maxLength) {
      return string;
    }

    if (string.length() >= 4) {
      return string.substring(0, string.length() - 3) + "...";
    } else {
      return string;
    }
  }

  private void updateCLIPrompt(ClientConfig clientConfig) {
    URI baseURI = clientConfig.getBaseURI();
    URI uri = baseURI.resolve("/" + clientConfig.getNamespace());
    cli.getReader().setPrompt("cdap (" + uri + ")> ");
  }

  public TableRenderer getTableRenderer() {
    return cliConfig.getTableRenderer();
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

  public static void main(String[] args) {
    final PrintStream output = System.out;

    Options options = getOptions();
    CommandLineParser parser = new BasicParser();
    try {
      CommandLine command = parser.parse(options, args);
      if (command.hasOption(HELP_OPTION.getOpt())) {
        usage();
        System.exit(0);
      }

      LaunchOptions launchOptions = LaunchOptions.builder()
        .setUri(command.getOptionValue(URI_OPTION.getOpt(), getDefaultURI()))
        .setDebug(command.hasOption(DEBUG_OPTION.getOpt()))
        .setVerifySSL(parseBooleanOption(command, VERIFY_SSL_OPTION, DEFAULT_VERIFY_SSL))
        .setAutoconnect(parseBooleanOption(command, AUTOCONNECT_OPTION, DEFAULT_AUTOCONNECT))
        .build();

      String[] commandArgs = command.getArgs();

      try {
        final CLIConfig cliConfig = new CLIConfig(ClientConfig.builder().build(),
                                                  output, new AltStyleTableRenderer());
        CLIMain cliMain = new CLIMain(launchOptions, cliConfig);
        CLI cli = cliMain.getCLI();

        if (commandArgs.length == 0) {
          cli.startInteractiveMode(output);
        } else {
          cli.execute(Joiner.on(" ").join(commandArgs), output);
        }
      } catch (Exception e) {
        e.printStackTrace(output);
      }
    } catch (ParseException e) {
      output.println(e.getMessage());
      usage();
    }
  }

  private static boolean parseBooleanOption(CommandLine command, Option option, boolean defaultValue) {
    String value = command.getOptionValue(option.getOpt(), Boolean.toString(defaultValue));
    return "true".equals(value);
  }

  private static Options getOptions() {
    Options options = new Options();
    addOptionalOption(options, HELP_OPTION);
    addOptionalOption(options, URI_OPTION);
    addOptionalOption(options, VERIFY_SSL_OPTION);
    addOptionalOption(options, AUTOCONNECT_OPTION);
    addOptionalOption(options, DEBUG_OPTION);
    return options;
  }

  private static void addOptionalOption(Options options, Option option) {
    OptionGroup optionalGroup = new OptionGroup();
    optionalGroup.setRequired(false);
    optionalGroup.addOption(option);
    options.addOptionGroup(optionalGroup);
  }

  private static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    String args =
      "[--autoconnect <true|false>] " +
      "[--debug] " +
      "[--help] " +
      "[--verify-ssl <true|false>] " +
      "[--uri <arg>]";
    formatter.printHelp("cdap-cli.sh " + args, getOptions());
    System.exit(0);
  }
}

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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import co.cask.cdap.cli.command.system.HelpCommand;
import co.cask.cdap.cli.command.system.SearchCommandsCommand;
import co.cask.cdap.cli.commandset.DefaultCommands;
import co.cask.cdap.cli.completer.supplier.EndpointSupplier;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.cli.util.table.AltStyleTableRenderer;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.client.exception.DisconnectedException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.utils.OSDetector;
import co.cask.common.cli.CLI;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import co.cask.common.cli.exception.CLIExceptionHandler;
import co.cask.common.cli.exception.InvalidCommandException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
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
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLHandshakeException;

/**
 * Main class for the CDAP CLI.
 */
public class CLIMain {

  private static final boolean DEFAULT_VERIFY_SSL = true;
  private static final boolean DEFAULT_AUTOCONNECT = true;
  private static final String TOOL_NAME = "cli";

  @VisibleForTesting
  public static final Option HELP_OPTION = new Option(
    "h", "help", false, "Print the usage message.");

  @VisibleForTesting
  public static final Option URI_OPTION = new Option(
    "u", "uri", true, "CDAP instance URI to interact with in" +
    " the format \"[http[s]://]<hostname>[:<port>[/<namespace>]]\"." +
    " Defaults to \"" + getDefaultURI().toString() + "\".");

  @VisibleForTesting
  public static final Option VERIFY_SSL_OPTION = new Option(
    "v", "verify-ssl", true, "If \"true\", verify SSL certificate when making requests." +
    " Defaults to \"" + DEFAULT_VERIFY_SSL + "\".");

  @VisibleForTesting
  public static final Option AUTOCONNECT_OPTION = new Option(
    "a", "autoconnect", true, "If \"true\", try provided connection" +
    " (from " + URI_OPTION.getLongOpt() + ")" +
    " upon launch or try default connection if none provided." +
    " Defaults to \"" + DEFAULT_AUTOCONNECT + "\".");

  @VisibleForTesting
  public static final Option DEBUG_OPTION = new Option(
    "d", "debug", false, "Print exception stack traces.");

  private static final Option SCRIPT_OPTION = new Option(
    "s", "script", true, "Execute a file containing a series of CLI commands, line-by-line.");

  private final CLI cli;
  private final Iterable<CommandSet<Command>> commands;
  private final CLIConfig cliConfig;
  private final Injector injector;
  private final LaunchOptions options;
  private final FilePathResolver filePathResolver;

  public CLIMain(final LaunchOptions options,
                 final CLIConfig cliConfig) throws URISyntaxException, IOException {
    this.options = options;
    this.cliConfig = cliConfig;

    injector = Guice.createInjector(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(LaunchOptions.class).toInstance(options);
          bind(CConfiguration.class).toInstance(CConfiguration.create());
          bind(PrintStream.class).toInstance(cliConfig.getOutput());
          bind(CLIConfig.class).toInstance(cliConfig);
          bind(ClientConfig.class).toInstance(cliConfig.getClientConfig());
        }
      }
    );

    this.commands = ImmutableList.of(
      injector.getInstance(DefaultCommands.class),
      new CommandSet<>(ImmutableList.<Command>of(
        new HelpCommand(getCommandsSupplier(), cliConfig),
        new SearchCommandsCommand(getCommandsSupplier(), cliConfig)
      )));
    filePathResolver = injector.getInstance(FilePathResolver.class);

    Map<String, Completer> completers = injector.getInstance(DefaultCompleters.class).get();
    cli = new CLI<>(Iterables.concat(commands), completers);
    cli.setExceptionHandler(new CLIExceptionHandler<Exception>() {
      @Override
      public boolean handleException(PrintStream output, Exception e, int timesRetried) {
        if (e instanceof SSLHandshakeException) {
          output.printf("To ignore this error, set \"--%s false\" when starting the CLI\n",
                        VERIFY_SSL_OPTION.getLongOpt());
        } else if (e instanceof InvalidCommandException) {
          InvalidCommandException ex = (InvalidCommandException) e;
          output.printf("Invalid command '%s'. Enter 'help' for a list of commands\n", ex.getInput());
        } else if (e instanceof DisconnectedException || e instanceof ConnectException) {
          cli.getReader().setPrompt("cdap (DISCONNECTED)> ");
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
    cli.getReader().setExpandEvents(false);
    cliConfig.addHostnameChangeListener(new CLIConfig.ConnectionChangeListener() {
      @Override
      public void onConnectionChanged(CLIConnectionConfig config) {
        updateCLIPrompt(config);
      }
    });
  }

  /**
   * Tries to autoconnect to the provided URI in options.
   */
  public boolean tryAutoconnect(CommandLine command) {
    if (!options.isAutoconnect()) {
      return true;
    }

    InstanceURIParser instanceURIParser = injector.getInstance(InstanceURIParser.class);
    try {
      CLIConnectionConfig connection = instanceURIParser.parse(options.getUri());
      cliConfig.tryConnect(connection, options.isVerifySSL(), cliConfig.getOutput(), options.isDebug());
      return true;
    } catch (Exception e) {
      if (options.isDebug()) {
        e.printStackTrace(cliConfig.getOutput());
      } else {
        cliConfig.getOutput().println(e.getMessage());
      }
      if (!command.hasOption(URI_OPTION.getOpt())) {
        cliConfig.getOutput().printf("Specify the CDAP instance URI with the -u command line argument.\n");
      }
      return false;
    }
  }

  public static URI getDefaultURI() {
    return ConnectionConfig.DEFAULT.getURI();
  }

  public FilePathResolver getFilePathResolver() {
    return filePathResolver;
  }

  private void updateCLIPrompt(CLIConnectionConfig config) {
    cli.getReader().setPrompt(getPrompt(config));
  }

  public String getPrompt(CLIConnectionConfig config) {
    try {
      return "cdap (" + config.getURI().resolve("/" + config.getNamespace()) + ")> ";
    } catch (DisconnectedException e) {
      return "cdap (DISCONNECTED)> ";
    }
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
    // disable logback logging
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.OFF);

    final PrintStream output = System.out;

    Options options = getOptions();
    CLIMainArgs cliMainArgs = CLIMainArgs.parse(args, options);

    CommandLineParser parser = new BasicParser();
    try {
      CommandLine command = parser.parse(options, cliMainArgs.getOptionTokens());
      if (command.hasOption(HELP_OPTION.getOpt())) {
        usage();
        System.exit(0);
      }

      LaunchOptions launchOptions = LaunchOptions.builder()
        .setUri(command.getOptionValue(URI_OPTION.getOpt(), getDefaultURI().toString()))
        .setDebug(command.hasOption(DEBUG_OPTION.getOpt()))
        .setVerifySSL(parseBooleanOption(command, VERIFY_SSL_OPTION, DEFAULT_VERIFY_SSL))
        .setAutoconnect(parseBooleanOption(command, AUTOCONNECT_OPTION, DEFAULT_AUTOCONNECT))
        .build();

      String scriptFile = command.getOptionValue(SCRIPT_OPTION.getOpt(), "");
      boolean hasScriptFile = command.hasOption(SCRIPT_OPTION.getOpt());

      String[] commandArgs = cliMainArgs.getCommandTokens();

      try {
        ClientConfig clientConfig = ClientConfig.builder()
          .setConnectionConfig(null)
          .setDefaultReadTimeout(60 * 1000)
          .build();
        final CLIConfig cliConfig = new CLIConfig(clientConfig, output, new AltStyleTableRenderer());
        CLIMain cliMain = new CLIMain(launchOptions, cliConfig);
        CLI cli = cliMain.getCLI();

        if (!cliMain.tryAutoconnect(command)) {
          System.exit(0);
        }

        CLIConnectionConfig connectionConfig = new CLIConnectionConfig(
          cliConfig.getClientConfig().getConnectionConfig(),
          cliConfig.getCurrentNamespace(), null);
        cliMain.updateCLIPrompt(connectionConfig);

        if (hasScriptFile) {
          File script = cliMain.getFilePathResolver().resolvePathToFile(scriptFile);
          if (!script.exists()) {
            output.println("ERROR: Script file '" + script.getAbsolutePath() + "' does not exist");
            System.exit(1);
          }
          List<String> scriptLines = Files.readLines(script, Charsets.UTF_8);
          for (String scriptLine : scriptLines) {
            output.print(cliMain.getPrompt(connectionConfig));
            output.println(scriptLine);
            cli.execute(scriptLine, output);
            output.println();
          }
        } else if (commandArgs.length == 0) {
          cli.startInteractiveMode(output);
        } else {
          cli.execute(Joiner.on(" ").join(commandArgs), output);
        }
      } catch (DisconnectedException e) {
        output.printf("Couldn't reach the CDAP instance at '%s'.", e.getConnectionConfig().getURI().toString());
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

  @VisibleForTesting
  public static Options getOptions() {
    Options options = new Options();
    addOptionalOption(options, HELP_OPTION);
    addOptionalOption(options, URI_OPTION);
    addOptionalOption(options, VERIFY_SSL_OPTION);
    addOptionalOption(options, AUTOCONNECT_OPTION);
    addOptionalOption(options, DEBUG_OPTION);
    addOptionalOption(options, SCRIPT_OPTION);
    return options;
  }

  private static void addOptionalOption(Options options, Option option) {
    OptionGroup optionalGroup = new OptionGroup();
    optionalGroup.setRequired(false);
    optionalGroup.addOption(option);
    options.addOptionGroup(optionalGroup);
  }

  private static void usage() {
    String toolName = "cdap" + (OSDetector.isWindows() ? ".bat " : " ") + TOOL_NAME;
    HelpFormatter formatter = new HelpFormatter();
    String args =
      "[--autoconnect <true|false>] " +
      "[--debug] " +
      "[--help] " +
      "[--verify-ssl <true|false>] " +
      "[--uri <uri>]" +
      "[--script <script-file>]";
    formatter.printHelp(toolName + " " + args, getOptions());
    System.exit(0);
  }

}

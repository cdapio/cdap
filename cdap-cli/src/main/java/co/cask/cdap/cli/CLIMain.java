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
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
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

  public static final String NAME_VERBOSE = "verbose";
  public static final String NAME_URI = "uri";
  public static final String NAME_VERIFY_SSL = "verify_ssl";
  public static final String NAME_AUTOCONNECT = "autoconnect";
  public static final String NAME_NAMESPACE = "namespace";

  private static final boolean DEFAULT_VERIFY_SSL = true;
  private static final boolean DEFAULT_AUTOCONNECT = true;
  private static final boolean DEFAULT_VERBOSE = false;
  private static final String DEFAULT_NAMESPACE = Constants.DEFAULT_NAMESPACE;

  private static final Option URI_OPTION = new Option(
    "u", "uri", true, "CDAP instance URI to interact with in" +
    " the format \"[http[s]://]<hostname>[:<port>[/<namespace>]]\"." +
    " Defaults to \"" + getDefaultURI() + "\".");

  private static final Option NAMESPACE_OPTION = new Option(
    "n", "namespace", true, "The CDAP namespace to use." +
    " Defaults to \"" + DEFAULT_NAMESPACE + "\".");

  private static final Option VERIFY_SSL_OPTION = new Option(
    "s", "verify-ssl", true, "If \"true\", verify SSL certificate when making requests." +
    " Defaults to \"" + DEFAULT_VERIFY_SSL + "\".");

  private static final Option AUTOCONNECT_OPTION = new Option(
    "a", "autoconnect", true, "If \"true\", try provided connection" +
    " (from " + URI_OPTION.getLongOpt() + ")" +
    " upon launch or try default connection if none provided." +
    " Defaults to \"" + DEFAULT_AUTOCONNECT + "\".");

  private static final Option VERBOSE_OPTION = new Option(
    "v", "verbose", true, "If \"true\", print all exception stack traces." +
    " Defaults to \"" + DEFAULT_VERBOSE + "\".");

  private final CLI cli;
  private final Iterable<CommandSet<Command>> commands;

  /**
   * @param output output to print to
   * @param namespace the CDAP namespace to use initially
   * @param uri provided URI of CDAP instance
   * @param autoconnect if true, try provided connection (or default from CConfiguration) before startup
   * @param verbose if true, log all exception stack traces
   * @throws URISyntaxException
   * @throws IOException
   */
  @Inject
  public CLIMain(Injector injector, PrintStream output,
                 @Named(NAME_NAMESPACE) String namespace,
                 @Named(NAME_URI) String uri,
                 @Named(NAME_AUTOCONNECT) boolean autoconnect,
                 @Named(NAME_VERBOSE) final boolean verbose,
                 CLIConfig cliConfig) throws URISyntaxException, IOException {

    if (autoconnect) {
      try {
        if (namespace != null) {
          cliConfig.getClientConfig().setNamespace(Id.Namespace.from(namespace));
        }
        cliConfig.getClientConfig().setURI(URI.create(uri));
        CLIConfig.ConnectionInfo connectionInfo = CLIConfig.ConnectionInfo.of(cliConfig.getClientConfig());
        cliConfig.tryConnect(connectionInfo, output, verbose);
      } catch (Exception e) {
        if (verbose) {
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
          output.printf("To ignore this error, set -D%s=false when starting the CLI\n", CLIConfig.PROP_VERIFY_SSL_CERT);
        } else if (e instanceof InvalidCommandException) {
          InvalidCommandException ex = (InvalidCommandException) e;
          output.printf("Invalid command '%s'. Enter 'help' for a list of commands\n", ex.getInput());
        } else {
          output.println("Error: " + e.getMessage());
        }

        if (verbose) {
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

  private void updateCLIPrompt(ClientConfig clientConfig) {
    URI baseURI = clientConfig.getBaseURI();
    URI uri = baseURI.resolve("/" + clientConfig.getNamespace());
    cli.getReader().setPrompt("cdap (" + uri + ")> ");
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
      final String uri = command.getOptionValue(URI_OPTION.getOpt(), getDefaultURI());
      final String namespace = command.getOptionValue(NAMESPACE_OPTION.getOpt(), DEFAULT_NAMESPACE);
      final boolean verifySSL = parseBooleanOption(command, VERIFY_SSL_OPTION, DEFAULT_VERIFY_SSL);
      final boolean verbose = parseBooleanOption(command, VERBOSE_OPTION, DEFAULT_VERBOSE);
      final boolean autoconnect = parseBooleanOption(command, AUTOCONNECT_OPTION, DEFAULT_AUTOCONNECT);
      String[] commandArgs = command.getArgs();

      try {
        ClientConfig clientConfig = new ClientConfig.Builder().setVerifySSLCert(verifySSL).build();
        final CLIConfig cliConfig = new CLIConfig(clientConfig);
        Injector injector = Guice.createInjector(
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(PrintStream.class).toInstance(output);
              bind(String.class).annotatedWith(Names.named(NAME_NAMESPACE)).toInstance(namespace);
              bind(String.class).annotatedWith(Names.named(NAME_URI)).toInstance(uri);
              bind(Boolean.class).annotatedWith(Names.named(NAME_VERIFY_SSL)).toInstance(verifySSL);
              bind(Boolean.class).annotatedWith(Names.named(NAME_VERBOSE)).toInstance(verbose);
              bind(Boolean.class).annotatedWith(Names.named(NAME_AUTOCONNECT)).toInstance(autoconnect);
              bind(CLIConfig.class).toInstance(cliConfig);
              bind(ClientConfig.class).toInstance(cliConfig.getClientConfig());
              bind(CConfiguration.class).toInstance(CConfiguration.create());
            }
          }
        );

        CLIMain cliMain = injector.getInstance(CLIMain.class);
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
    addOptionalOption(options, URI_OPTION);
    addOptionalOption(options, VERIFY_SSL_OPTION);
    addOptionalOption(options, NAMESPACE_OPTION);
    addOptionalOption(options, AUTOCONNECT_OPTION);
    addOptionalOption(options, VERBOSE_OPTION);
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
    formatter.printHelp("CDAP CLI", getOptions());
    System.exit(0);
  }
}

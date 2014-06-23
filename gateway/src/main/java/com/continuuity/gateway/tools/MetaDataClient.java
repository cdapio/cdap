package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.UsageException;
import com.continuuity.gateway.util.Util;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;

import java.io.PrintStream;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Command line meta data client.
 */
public class MetaDataClient extends ClientToolBase {

  /**
   * for debugging. should only be set to true in unit tests.
   * when true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  private static final String APP_OPTION = "app";
  private static final String TYPE_OPTION = "type";
  private static final String ID_OPTION = "id";
  private static final String FILTER_OPTION = "filter";
  private static final String VALUE_OPTION = "value";

  String app = null;             // the application to inspect, optional
  String type = null;            // the type of entries
  String id = null;              // the id of the entry to show, optional

  LinkedList<String> filters = Lists.newLinkedList(); // filter fields
  LinkedList<String> values = Lists.newLinkedList();  // corresponding values

  public MetaDataClient() {
    super("meta-client");
    buildOptions();
  }

  public MetaDataClient(String toolName) {
    super(toolName);
    buildOptions();
  }

  @Override
  public void buildOptions() {
    // build the default options
    super.buildOptions();
    options.addOption(OptionBuilder.withLongOpt(FILTER_OPTION)
                        .hasArg(true)
                        .withDescription("To specify a field to filter on")
                        .create());
    options.addOption(OptionBuilder.withLongOpt(VALUE_OPTION)
                        .hasArg(true)
                        .withDescription("To specify a value to filter on")
                        .create());
    options.addOption(null, APP_OPTION, true, "To specify the application to inspect");
    options.addOption(null, TYPE_OPTION, true, "To specify the type of entries");
    options.addOption(null, ID_OPTION, true, "The id of the entry to show");
  }

  @Override
  public void printUsage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    out.println("Usage: ");
    out.println("\t" + getToolName() + " list [ --application <id> ] --type <name>");
    out.println("\t" + getToolName() + " read [ --application <id> ] --type <name> --id <id>\n");
    super.printUsage(error);
  }

  /**
   * Parse the command line arguments.
   */
  protected boolean parseArguments(String[] args) {
    // parse generic args first
    CommandLineParser parser = new GnuParser();
    // Check all the options of the command line
    try {
      command = args[0];
      CommandLine line = parser.parse(options, args);
      parseBasicArgs(line);
      if (line.hasOption(FILTER_OPTION)) {
        String[] filterList = line.getOptionValues(FILTER_OPTION);
        for (int i = 0; i < filterList.length; ++i) {
          filters.add(filterList[i]);
        }
      }
      if (line.hasOption(VALUE_OPTION)) {
        String[] valueList = line.getOptionValues(VALUE_OPTION);
        for (int i = 0; i < valueList.length; ++i) {
          values.add(valueList[i]);
        }
      }
      app = line.getOptionValue(APP_OPTION, null);
      type = line.getOptionValue(TYPE_OPTION, null);
      id = line.getOptionValue(ID_OPTION, null);

      // expect at least 1 extra arg because of pos arg, the command to run
      if (line.getArgs().length > 1) {
        usage("Extra arguments provided");
      }
    } catch (ParseException e) {
      printUsage(true);
    } catch (IndexOutOfBoundsException e) {
      printUsage(true);
    }
    return true;
  }

  static List<String> supportedCommands = Arrays.asList("list", "read");

  protected void validateArguments(String[] args) {
    // first parse command arguments
    parseArguments(args);
    if (help) {
      return;
    }

    // first validate the command
    if (!supportedCommands.contains(command)) {
      usage("Please provide a valid command.");
    }
    if (type == null) {
      usage("--type must be specified");
    }
    if ("read".equals(command)) {
      if (id == null) {
        usage("--id must be specified");
      }
      if (!filters.isEmpty()) {
        usage("--filter is not allowed with read");
      }
    } else {
      if (id != null) {
        usage("--id is not alllowed with list");
      }
    }
    if (filters.size() != values.size()) {
      usage("number of --filter and --value does not match");
    }
  }

  public String execute0(String[] args, CConfiguration config) {
    // parse and validate arguments
    validateArguments(args);
    if (help) {
      return "";
    }
    if (accessToken == null && tokenFile != null) {
      readTokenFile();
    }
    boolean useSsl = !forceNoSSL && (apikey != null);
    String baseUrl = GatewayUrlGenerator.getBaseUrl(config, hostname, port, useSsl);
    if (baseUrl == null) {
      System.err.println("Can't figure out the URL to send to. " +
                         "Please use --host and --port to specify.");
      return null;
    } else {
      if (verbose) {
        System.out.println("Using base URL: " + baseUrl);
      }
    }

    // construct the full URL and verify its well-formedness
    try {
      URI.create(baseUrl);
    } catch (IllegalArgumentException e) {
      // this can only happen if the --host, or --base are not valid for a URL
      System.err.println("Invalid base URL '" + baseUrl + "'. Check the validity of --host or --port arguments.");
      return null;
    }

    String requestUri = baseUrl + type;
    if (id != null) {
      requestUri += "/" + id;
    }
    String sep = "?";
    if (app != null) {
      requestUri += sep + "application=" + app;
      sep = "&";
    }
    while (!filters.isEmpty()) {
      requestUri += sep + filters.removeFirst() + "=" + values.removeFirst();
      sep = "&";
    }
    HttpGet get = new HttpGet(requestUri);
    HttpResponse response = sendHttpRequest(get, null);
    if (printResponse(response) == null) {
      return null;
    }
    return "OK.";
  }

  public String printResponse(HttpResponse response) {
    // read the binary value from the HTTP response
    if (response == null) {
      return null;
    }
    byte[] binaryResponse = Util.readHttpResponse(response);
    if (binaryResponse == null) {
      return null;
    }
    // now make returned value available to user
    System.out.println(new String(binaryResponse, Charsets.UTF_8));
    return "OK.";
  }

  public String execute(String[] args, CConfiguration config) {
    try {
      return execute0(args, config);
    } catch (UsageException e) {
      if (debug) { // this is mainly for debugging the unit test
        System.err.println("Exception for arguments: " + Arrays.toString(args) + ". Exception: " + e);
        e.printStackTrace(System.err);
      }
    }
    return null;
  }

  /**
   * This is the main method. It delegates to getValue() in order to make
   * it possible to test the return value.
   */
  public static void main(String[] args) {
    // create a config and load the gateway properties
    CConfiguration config = CConfiguration.create();
    // create a data client and run it with the given arguments
    MetaDataClient instance = new MetaDataClient();
    String value = instance.execute(args, config);
    // exit with error in case fails
    if (value == null) {
      System.exit(1);
    }
  }
}


package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.util.Util;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

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

  String command = null;         // the command to run
  String app = null;             // the application to inspect, optional
  String type = null;            // the type of entries
  String id = null;              // the id of the entry to show, optional

  LinkedList<String> filters = Lists.newLinkedList(); // filter fields
  LinkedList<String> values = Lists.newLinkedList();  // corresponding values

  public MetaDataClient() {
    super("meta-client");
  }

  public MetaDataClient(String toolName) {
    super(toolName);
  }

  @Override
  protected void addOptions(Options options) {
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
  public void printUsageTop(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    out.println("Usage: ");
    out.println("\t" + getToolName() + " list [ --application <id> ] --type <name>");
    out.println("\t" + getToolName() + " read [ --application <id> ] --type <name> --id <id>\n");
  }

  @Override
  protected boolean parseAdditionalArguments(CommandLine line) {
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
    // should have 1 arg remaining, the command which is positional
    String[] remaining = line.getArgs();
    if (remaining.length != 1) {
      return false;
    }
    command = remaining[0];
    return true;
  }

  static List<String> supportedCommands = Arrays.asList("list", "read");

  @Override
  protected String validateArguments() {
    if (!supportedCommands.contains(command)) {
      return "Please provide a valid command.";
    }
    if (type == null) {
      return "--type must be specified";
    }
    if ("read".equals(command)) {
      if (id == null) {
        return "--id must be specified";
      }
      if (!filters.isEmpty()) {
        return "--filter is not allowed with read";
      }
    } else {
      if (id != null) {
        return "--id is not alllowed with list";
      }
    }
    if (filters.size() != values.size()) {
      return "number of --filter and --value does not match";
    }
    return null;
  }

  protected String execute(CConfiguration config) {
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
    HttpClient client = new DefaultHttpClient();

    try {
      HttpResponse response = sendHttpRequest(client, get, null);
      if (printResponse(response) == null) {
        return null;
      }
      return "OK.";
    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  /**
   * Prints the contents of HTTP response if it is not null.
   *
   * @param response The HttpResponse to print
   * @return String specifying whether the procedure succeeded or null otherwise.
   */
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


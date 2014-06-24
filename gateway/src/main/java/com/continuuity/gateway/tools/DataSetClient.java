package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.util.Util;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DataSetClient extends ClientToolBase {

  private static final String TABLE_OPTION = "table";
  private static final String ROW_OPTION = "row";
  private static final String COLUMN_OPTION = "column";
  private static final String COLUMNS_OPTION = "columns";
  private static final String VALUE_OPTION = "value";
  private static final String VALUES_OPTION = "values";
  private static final String START_OPTION = "start";
  private static final String STOP_OPTION = "stop";
  private static final String LIMIT_OPTION = "limit";
  private static final String HEX_OPTION = "hex";
  private static final String URL_OPTION = "url";
  private static final String BASE_64_OPTION = "base64";
  private static final String COUNTER_OPTION = "counter";
  private static final String JSON_OPTION = "json";
  private static final String PRETTY_OPTION = "pretty";

  private String command = null;  // the command to run
  boolean pretty = true;         // for pretty-printing
  String row = null;             // the row to read/write/delete/increment
  LinkedList<String> columns = Lists.newLinkedList(); // the columns to read/delete/write/increment
  LinkedList<String> values = Lists.newLinkedList(); // the values to write/increment
  String encoding = null;        // the encoding for row keys, column keys, values
  boolean counter = false;         // to interpret values as counters
  String table = null;           // the name of the table to operate on
  String startcol = null;        // the column to start a range
  String stopcol = null;         // the column to end a range
  int limit = -1;                // the limit for a range

  public DataSetClient() {
    super("data-client");
  }

  public DataSetClient(String toolName) {
    super(toolName);
  }

  @Override
  public void addOptions(Options options) {
    options.addOption(null, TABLE_OPTION, true, "To specify the table to operate on");
    options.addOption(null, ROW_OPTION, true, "To specify the row to operate on");
    options.addOption(null, COLUMN_OPTION, true, "To specify a single columns to operate on");
    options.addOption(OptionBuilder.withLongOpt(COLUMNS_OPTION)
                        .hasArgs()
                        .withDescription("To specify a list of columns to operate on.\n--" +
                                           COLUMNS_OPTION + " column1 column2 ...")
                        .create());
    options.addOption(null, VALUE_OPTION, true, "To specify a single value to write/increment");
    options.addOption(OptionBuilder.withLongOpt(VALUES_OPTION)
                        .hasArgs()
                        .withDescription("To specify a list of values to operate on.\n--" +
                                           VALUES_OPTION + " value1 value2 ...")
                        .create());
    options.addOption(null, START_OPTION, true, "To specify the start of a column range");
    options.addOption(null, STOP_OPTION, true, "To specify the end of a column range");
    options.addOption(null, LIMIT_OPTION, true, "To specify a limit for a column range");
    options.addOption(null, HEX_OPTION, false, "To specify hex encoding for keys/values");
    options.addOption(null, URL_OPTION, false, "To specify url encoding for keys/values");
    options.addOption(null, BASE_64_OPTION, false, "To specify base64 encoding for keys/values");
    options.addOption(null, COUNTER_OPTION, false, "To interpret values as long counters");
    options.addOption(null, JSON_OPTION, false, "To see the raw JSON output");
    options.addOption(null, PRETTY_OPTION, false, "To see pretty printed output");
  }

  @Override
  public void printUsageTop(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    out.println("Usage: ");
    out.println("\t" + getToolName() + " create --table name");
    out.println("\t" + getToolName() + " read --table name --row <row key> [ <option> ... ]");
    out.println("\t" + getToolName() + " write --table name --row <row key> [ <option> ... ]");
    out.println("\t" + getToolName() + " increment --table name --row <row key> [ <option> ... ]");
    out.println("\t" + getToolName() + " delete --table name --row <row key> [ <option> ... ]");
    out.println("\t" + getToolName() + " clear --table name [ <option> ... ]\n");
  }

  @Override
  protected boolean parseAdditionalArguments(CommandLine line) {
    // Check all the options of the command line
    // try to get the value and use default value if we cant get the value
    table = line.getOptionValue(TABLE_OPTION, null);
    row = line.getOptionValue(ROW_OPTION, null);
    startcol = line.getOptionValue(START_OPTION, null);
    stopcol = line.getOptionValue(STOP_OPTION, null);
    counter = line.hasOption(COUNTER_OPTION);
    encoding = line.hasOption(HEX_OPTION) ? "hex" : encoding;
    encoding = line.hasOption(URL_OPTION) ? "url" : encoding;
    encoding = line.hasOption(BASE_64_OPTION) ? "base64" : encoding;
    // either pretty or json is allowed
    pretty = line.hasOption(PRETTY_OPTION) || pretty;
    pretty = line.hasOption(JSON_OPTION) || pretty;
    limit = line.hasOption(LIMIT_OPTION) ? parseNumericArg(line, LIMIT_OPTION).intValue() : -1;
    if (line.hasOption(COLUMN_OPTION)) {
      String[] columnList = line.getOptionValues(COLUMN_OPTION);
      Collections.addAll(columns, columnList);
    }
    if (line.hasOption(COLUMNS_OPTION)) {
      String[] columnList = line.getOptionValues(COLUMNS_OPTION);
      Collections.addAll(columns, columnList);
    }
    if (line.hasOption(VALUE_OPTION)) {
      String[] valueList = line.getOptionValues(VALUE_OPTION);
      Collections.addAll(values, valueList);
    }
    if (line.hasOption(VALUES_OPTION)) {
      String[] valueList = line.getOptionValues(VALUES_OPTION);
      Collections.addAll(values, valueList);
    }
    // should have 1 arg remaining, the command which is positional
    String[] remaining = line.getArgs();
    if (remaining.length != 1) {
      return false;
    }
    command = remaining[0];
    return true;
  }

  static List<String> supportedCommands =
    Arrays.asList("read", "write", "increment", "delete", "create", "clear");

  @Override
  protected String validateArguments() {
    // first validate the command
    if (!supportedCommands.contains(command)) {
      return "Please enter a valid command.";
    }

    if (table == null && !"clear".equals(command)) {
      return "--table is required for table operations.";
    }

    if ("create".equals(command) || "clear".equals(command)) {
      if (row != null) {
        return "--row is not allowed for table " + command + " command.";
      }
      if (!columns.isEmpty() || startcol != null || stopcol != null || limit != -1 || !values.isEmpty()) {
        return "specifying columns or values is not allowed for table " + command + " command.";
      }
    } else {
      if (row == null) {
        return "--row is required for table operations.";
      }
      if ("read".equals(command)) {
        if ((startcol != null || stopcol != null) && !columns.isEmpty()) {
          return "--start/stop and --column(s) may not be used together.";
        }
      } else { // table op but not read
        if (columns.isEmpty()) {
          return "--column(s) is required for table write operations.";
        }
        if (!"delete".equals(command)) {
          if (columns.size() != values.size()) {
            return "number of values must match number of columns.";
          }
        }
        if ("increment".equals(command)) {
          try {
            for (String value : values) {
              Long.parseLong(value);
            }
          } catch (NumberFormatException e) {
            return "for increment all values must be numbers";
          }
        }
      }
    }
    if (port > 0 && hostname == null) {
      return "A hostname must be provided when a port is specified.";
    }
    return null;
  }

  /**
   * This is actually the main method, but in order to make it testable,
   * instead of exiting in case of error it returns null, whereas in case of
   * success it returns the retrieved value as shown
   * on the console.
   *
   * @param config The configuration of the gateway
   * @return null in case of error, an string representing the retrieved value
   *         in case of success
   */
  public String execute0(CConfiguration config) {
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

    // prepare for HTTP
    HttpResponse response;

    // construct the full URL and verify its well-formedness
    try {
      URI.create(baseUrl);
    } catch (IllegalArgumentException e) {
      // this can only happen if the --host, or --base are not valid for a URL
      System.err.println("Invalid base URL '" + baseUrl + "'. Check the validity of --host or --port arguments.");
      return null;
    }

    // must be a table operation
    String requestUrl = baseUrl + "tables/" + table;
    if ("create".equals(command)) {
      HttpClient client = new DefaultHttpClient();
      // url is already complete, submit as a put
      HttpPut put = new HttpPut(requestUrl);
      try {
        return (sendHttpRequest(client, put, null) != null) ? "OK." : null;
      } finally {
        client.getConnectionManager().shutdown();
      }
    }
    if ("clear".equals(command)) {
      HttpClient client = new DefaultHttpClient();
      // url is already complete, submit as a post
      HttpPost post = new HttpPost(baseUrl + "datasets/" + table + "/truncate");
      try {
        return (sendHttpRequest(client, post, null) != null) ? "OK." : null;
      } finally {
        client.getConnectionManager().shutdown();
      }
    }
    // all operations other than create require row
    requestUrl += "/rows/" + row;
    String sep = "?";
    if ("read".equals(command)) {
      if (startcol != null) {
        requestUrl += sep + "start=" + startcol;
        sep = "&";
      }
      if (stopcol != null) {
        requestUrl += sep + "stop=" + stopcol;
        sep = "&";
      }
      if (limit != -1) {
        requestUrl += sep + "limit=" + limit;
        sep = "&";
      }
      if (!columns.isEmpty()) {
        String pre = sep + "columns=";
        for (String column : columns) {
          requestUrl += pre + column;
          pre = ",";
        }
        sep = "&";
      }
      if (encoding != null) {
        requestUrl += sep + "encoding=" + encoding;
        sep = "&";
      }
      if (counter) {
        requestUrl += sep + "counter=1";
      }
      // now execute this as a get
      HttpClient client = new DefaultHttpClient();
      HttpGet get = new HttpGet(requestUrl);
      try {
        response = sendHttpRequest(client, get, null);
        if (response == null) {
          return null;
        }
        return printResponse(response);
      } finally {
        client.getConnectionManager().shutdown();
      }
    }

    if ("write".equals(command)) {
      if (encoding != null) {
        requestUrl += sep + "encoding=" + encoding;
        sep = "&";
      }
      if (counter) {
        requestUrl += sep + "counter=1";
      }
      // request URL is complete - construct the Json body
      byte[] requestBody = buildJson(false);

      // url and body ready, now submit as put
      HttpClient client = new DefaultHttpClient();
      HttpPut put = new HttpPut(requestUrl);
      put.setEntity(new ByteArrayEntity(requestBody));
      try {
        return (sendHttpRequest(client, put, null) != null) ? "OK." : null;
      } finally {
        client.getConnectionManager().shutdown();
      }
    }

    if ("increment".equals(command)) {
      requestUrl += "/increment?";
      if (encoding != null) {
        requestUrl += "encoding=" + encoding;
      }
      // request URL is complete - construct the Json body
      byte[] requestBody = buildJson(true);

      // url and body ready, now submit as post
      HttpClient client = new DefaultHttpClient();
      HttpPost post = new HttpPost(requestUrl);
      post.setEntity(new ByteArrayEntity(requestBody));
      try {
        response = sendHttpRequest(client, post, null);
        if (response == null) {
          return null;
        }
        return printResponse(response);
      } finally {
        client.getConnectionManager().shutdown();
      }
    }

    if ("delete".equals(command)) {
      String pre = "?columns=";
      for (String column : columns) {
        requestUrl += pre + column;
        pre = ",";
      }
      if (encoding != null) {
        requestUrl += "&encoding=" + encoding;
      }
      // url is ready, now submit as delete
      HttpClient client = new DefaultHttpClient();
      HttpDelete delete = new HttpDelete(requestUrl);
      try {
        return (sendHttpRequest(client, delete, null) != null) ? "OK." : null;
      } finally {
        client.getConnectionManager().shutdown();
      }
    }
    return null;
  }

  byte[] buildJson(boolean isNumber) {
    if (columns.isEmpty()) {
      return new byte[]{'{', '}'};
    }

    StringBuilder builder = new StringBuilder();
    char pre = '{';
    while (!columns.isEmpty()) {
      builder.append(pre);
      builder.append('"');
      builder.append(columns.removeFirst());
      builder.append("\":");
      if (!isNumber) {
        builder.append('"');
      }
      builder.append(values.removeFirst());
      if (!isNumber) {
        builder.append('"');
      }
      pre = ',';
    }
    builder.append('}');
    return builder.toString().getBytes(Charsets.UTF_8);
  }

  public String printResponse(HttpResponse response) {
    if (pretty) {
      try {
        if (response.getEntity().getContent() == null) {
          return null;
        }
        Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
        Type stringMapType = new TypeToken<Map<String, String>>() { }.getType();
        Map<String, String> map = new Gson().fromJson(reader, stringMapType);
        String value = null;
        for (Map.Entry<String, String> entry : map.entrySet()) {
          if (value == null) {
            value = entry.getValue();
          }
          System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        return value == null ? "<null>" : value;
      } catch (IOException e) {
        System.err.println("Error reading HTTP response: " + e.getMessage());
        return null;
      }
    } else {
      // read the binary value from the HTTP response
      byte[] binaryResponse = Util.readHttpResponse(response);
      if (binaryResponse == null) {
        return null;
      }
      // now make returned value available to user
      System.out.println(new String(binaryResponse, Charsets.UTF_8));
      return "OK.";
    }
  }

  public String execute(CConfiguration config) {
    return execute0(config);
  }

  /**
   * This is the main method. It delegates to getValue() in order to make
   * it possible to test the return value.
   */
  public static void main(String[] args) {
      // create a config and load the gateway properties
    CConfiguration config = CConfiguration.create();
    // create a data client and run it with the given arguments
    DataSetClient instance = new DataSetClient();
    String value = instance.execute(args, config);
    // exit with error in case fails
    if (value == null) {
      System.exit(1);
    }
  }
}

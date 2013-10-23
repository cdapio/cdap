package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.Copyright;
import com.continuuity.common.utils.UsageException;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.Util;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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
public class DataSetClient {

  static {
    // this turns off all logging but we don't need that for a cmdline tool
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  /**
   * for debugging. should only be set to true in unit tests.
   * when true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  boolean help = false;          // whether --help was there
  boolean verbose = false;       // for debug output
  boolean pretty = true;         // for pretty-printing
  String command = null;         // the command to run

  String hostname = null;        // the hostname of the gateway
  int port = -1;                 // the port of the gateway
  String apikey = null;          // the api key for authentication.

  String row = null;             // the row to read/write/delete/increment
  LinkedList<String> columns = Lists.newLinkedList(); // the columns to read/delete/write/increment
  LinkedList<String> values = Lists.newLinkedList(); // the values to write/increment
  String encoding = null;        // the encoding for row keys, column keys, values
  boolean counter = false;         // to interpret values as counters

  String table = null;           // the name of the table to operate on

  String startcol = null;        // the column to start a range
  String stopcol = null;         // the column to end a range
  int limit = -1;                // the limit for a range

  boolean forceNoSSL = false;    // to disable SSL even with api key and remote host

  public DataSetClient disallowSSL() {
    this.forceNoSSL = true;
    return this;
  }

  /**
   * Print the usage statement and return null (or empty string if this is not
   * an error case). See getValue() for an explanation of the return type.
   *
   * @param error indicates whether this was invoked as the result of an error
   * @throws com.continuuity.common.utils.UsageException
   *          in case of error
   */
  void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = "data-client";
    if (System.getProperty("script") != null) {
      name = System.getProperty("script").replaceAll("[./]", "");
    }
    Copyright.print(out);
    out.println("Usage: ");
    out.println("  " + name + " create --table name");
    out.println("  " + name + " read --table name --row <row key> ...");
    out.println("  " + name + " write --table name --row <row key> ...");
    out.println("  " + name + " increment --table name --row <row key> ...");
    out.println("  " + name + " delete --table name --row <row key> ...");
    out.println("  " + name + " clear --table name ...");
    out.println();
    out.println("Additional options:");
    out.println("  --table <name>          To specify the table to operate on");
    out.println("  --row <row key>         To specify the row to operate on");
    out.println("  --column <key>          To specify a single columns to operate on");
    out.println("  --columns <key,...>     To specify a list of columns to operate on");
    out.println("  --value <value>         To specify a single value to write/increment");
    out.println("  --values <value,...>    To specify a list of values to operate on");
    out.println("  --start <key>           To specify the start of a column range");
    out.println("  --stop <key>            To specify the end of a column range");
    out.println("  --limit <number>        To specify a limit for a column range");
    out.println("  --hex                   To specify hex encoding for keys/values");
    out.println("  --url                   To specify hex encoding for keys/values");
    out.println("  --base64                To specify base64 encoding for keys/values");
    out.println("  --counter               To interpret values as long counters");
    out.println("  --host <name>           To specify the hostname to send to");
    out.println("  --port <number>         To specify the port to use");
    out.println("  --apikey <apikey>       To specify an API key for authentication");
    out.println("  --json                  To see the raw JSON output");
    out.println("  --pretty                To see pretty printed output");
    out.println("  --verbose               To see more verbose output");
    out.println("  --help                  To print this message");
    if (error) {
      throw new UsageException();
    }
  }

  /**
   * Print an error message followed by the usage statement.
   *
   * @param errorMessage the error message
   */
  void usage(String errorMessage) {
    if (errorMessage != null) {
      System.err.println("Error: " + errorMessage);
    }
    usage(true);
  }


  /**
   * Parse the command line arguments.
   */
  void parseArguments(String[] args) {
    if (args.length == 0) {
      usage(true);
    }
    if ("--help".equals(args[0])) {
      usage(false);
      help = true;
      return;
    } else {
      command = args[0];
    }
    // go through all the arguments
    for (int pos = 1; pos < args.length; pos++) {
      String arg = args[pos];
      if ("--host".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        hostname = args[pos];
      } else if ("--port".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        try {
          port = Integer.parseInt(args[pos]);
        } catch (NumberFormatException e) {
          usage(true);
        }
      } else if ("--apikey".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        apikey = args[pos];
      } else if ("--table".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        table = args[pos];
      } else if ("--row".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        row = args[pos];
      } else if ("--start".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        startcol = args[pos];
      } else if ("--stop".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        stopcol = args[pos];
      } else if ("--limit".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        try {
          limit = Integer.parseInt(args[pos]);
        } catch (NumberFormatException e) {
          usage(true);
        }
      } else if ("--column".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        columns.add(args[pos]);
      } else if ("--columns".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        Collections.addAll(columns, args[pos].split(","));
      } else if ("--value".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        values.add(args[pos]);
      } else if ("--values".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        Collections.addAll(values, args[pos].split(","));
      } else if ("--counter".equals(arg)) {
        counter = true;
      } else if ("--hex".equals(arg)) {
        encoding = "hex";
      } else if ("--url".equals(arg)) {
        encoding = "url";
      } else if ("--base64".equals(arg)) {
        encoding = "base64";
      } else if ("--verbose".equals(arg)) {
        verbose = true;
      } else if ("--pretty".equals(arg)) {
        pretty = true;
      } else if ("--json".equals(arg)) {
        pretty = false;
      } else if ("--help".equals(arg)) {
        help = true;
        usage(false);
        return;
      } else {  // unknown argument
        usage(true);
      }
    }
  }

  static List<String> supportedCommands =
    Arrays.asList("read", "write", "increment", "delete", "create", "clear");

  void validateArguments(String[] args) {
    // first parse command arguments
    parseArguments(args);
    if (help) {
      return;
    }

    // first validate the command
    if (!supportedCommands.contains(command)) {
      usage("Unsupported command '" + command + "'.");
    }

    if (table == null && !"clear".equals(command)) {
      usage("--table is required for table operations.");
    }

    if ("create".equals(command) || "clear".equals(command)) {
      if (row != null) {
        usage("--row is not allowed for table " + command + " command.");
      }
      if (!columns.isEmpty() || startcol != null || stopcol != null || limit != -1 || !values.isEmpty()) {
        usage("specifying columns or values is not allowed for table " + command + " command.");
      }
    } else {
      if (row == null) {
        usage("--row is required for table operations.");
      }
      if ("read".equals(command)) {
        if ((startcol != null || stopcol != null) && !columns.isEmpty()) {
          usage("--start/stop and --column(s) may not be used together.");
        }
      } else { // table op but not read
        if (columns.isEmpty()) {
          usage("--column(s) is required for table write operations.");
        }
        if (!"delete".equals(command)) {
          if (columns.size() != values.size()) {
            usage("number of values must match number of columns.");
          }
        }
        if ("increment".equals(command)) {
          try {
            for (String value : values) {
              Long.parseLong(value);
            }
          } catch (NumberFormatException e) {
            usage("for increment all values must be numbers");
          }
        }
      }
    }
    if (port > 0 && hostname == null) {
      usage("A hostname must be provided when a port is specified.");
    }
  }

  /**
   * This is actually the main method, but in order to make it testable,
   * instead of exiting in case of error it returns null, whereas in case of
   * success it returns the retrieved value as shown
   * on the console.
   *
   * @param args   the command line arguments of the main method
   * @param config The configuration of the gateway
   * @return null in case of error, an string representing the retrieved value
   *         in case of success
   */
  public String execute0(String[] args, CConfiguration config) {
    // parse and validate arguments
    validateArguments(args);
    if (help) {
      return "";
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

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
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
      // url is already complete, submit as a put
      try {
        HttpPut put = new HttpPut(requestUrl);
        if (apikey != null) {
          put.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, apikey);
        }
        response = client.execute(put);
        client.getConnectionManager().shutdown();
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }
      if (!checkHttpStatus(response)) {
        return null;
      }
      return "OK.";
    }
    if ("clear".equals(command)) {
      // url is already complete, submit as a put
      try {
        HttpPost post = new HttpPost(baseUrl + "datasets/" + table + "/truncate");
        if (apikey != null) {
          post.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, apikey);
        }
        response = client.execute(post);
        client.getConnectionManager().shutdown();
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }
      if (!checkHttpStatus(response)) {
        return null;
      }
      return "OK.";
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
      try {
        HttpGet get = new HttpGet(requestUrl);
        if (apikey != null) {
          get.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, apikey);
        }
        response = client.execute(get);
        client.getConnectionManager().shutdown();
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }
      if (!checkHttpStatus(response)) {
        return null;
      }
      return printResponse(response);
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
      try {
        HttpPut put = new HttpPut(requestUrl);
        put.setEntity(new ByteArrayEntity(requestBody));
        if (apikey != null) {
          put.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, apikey);
        }
        response = client.execute(put);
        client.getConnectionManager().shutdown();
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }
      if (!checkHttpStatus(response)) {
        return null;
      }
      return "OK.";
    }

    if ("increment".equals(command)) {
      requestUrl += "/increment?";
      if (encoding != null) {
        requestUrl += "encoding=" + encoding;
      }
      // request URL is complete - construct the Json body
      byte[] requestBody = buildJson(true);

      // url and body ready, now submit as post
      try {
        HttpPost post = new HttpPost(requestUrl);
        post.setEntity(new ByteArrayEntity(requestBody));
        if (apikey != null) {
          post.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, apikey);
        }
        response = client.execute(post);
        client.getConnectionManager().shutdown();
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }
      if (!checkHttpStatus(response)) {
        return null;
      }
      return printResponse(response);
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
      try {
        HttpDelete delete = new HttpDelete(requestUrl);
        if (apikey != null) {
          delete.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, apikey);
        }
        response = client.execute(delete);
        client.getConnectionManager().shutdown();
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }
      if (!checkHttpStatus(response)) {
        return null;
      }
      return "OK.";
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

  /**
   * Check whether the Http return code is positive. If not, print the error
   * message and return false. Otherwise, if verbose is on, print the response
   * status line.
   *
   * @param response the HTTP response
   * @return whether the response indicates success
   */
  boolean checkHttpStatus(HttpResponse response) {
    try {
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        // get the error message from the body of the response
        String reason = response.getEntity() == null || response.getEntity().getContent() == null ? null :
          IOUtils.toString(response.getEntity().getContent());
        if (verbose) {
          System.out.println(response.getStatusLine());
          if (reason != null && !reason.isEmpty()) {
            System.out.println(reason);
          }
        } else {
          if (reason != null && !reason.isEmpty()) {
            System.err.println(response.getStatusLine().getReasonPhrase() + ": " + reason);
          } else {
            System.err.println(response.getStatusLine().getReasonPhrase());
          }
        }
        return false;
      }
      if (verbose) {
        System.out.println(response.getStatusLine());
      }
      return true;
    } catch (IOException e) {
      System.err.println("Error reading HTTP response: " + e.getMessage());
      return false;
    }
  }

  public String printResponse(HttpResponse response) {
    if (pretty) {
      try {
        if (response.getEntity().getContent() == null) {
          return null;
        }
        Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
        Type stringMapType = new TypeToken<Map<String, String>>() {}.getType();
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
    DataSetClient instance = new DataSetClient();
    String value = instance.execute(args, config);
    // exit with error in case fails
    if (value == null) {
      System.exit(1);
    }
  }
}

package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.UsageException;
import com.continuuity.gateway.util.Util;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Command line meta data client.
 */
public class MetaDataClient {

  static {
    // this turns off all logging but we don't need that for a cmdline tool
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  private static final Gson GSON = new Gson();
  /**
   * for debugging. should only be set to true in unit tests.
   * when true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  boolean help = false;          // whether --help was there
  boolean verbose = false;       // for debug output

  String hostname = null;        // the hostname of the gateway
  int port = -1;                 // the port of the gateway
  String apikey = null;          // the api key for authentication.
  String accessToken = null;     // the access token for connecting to secure reactor
  String tokenFile = null;       // path to file which contains an access token

  String command = null;         // the command to run

  String app = null;             // the application to inspect, optional
  String type = null;            // the type of entries
  String id = null;              // the id of the entry to show, optional

  boolean forceNoSSL = false;    // to disable SSL even with api key and remote host

  LinkedList<String> filters = Lists.newLinkedList(); // filter fields
  LinkedList<String> values = Lists.newLinkedList();  // corresponding values

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
    String name = "meta-client";
    if (System.getProperty("script") != null) {
      name = System.getProperty("script").replaceAll("[./]", "");
    }
    out.println("Usage: ");
    out.println("  " + name + " list [ --application <id> ] --type <name>");
    out.println("  " + name + " read [ --application <id> ] --type <name> --id <id>");
    out.println();
    out.println("Additional options:");
    out.println("  --filter <name>         To specify a field to filter on");
    out.println("  --value <name>          To specify a value to filter on");
    out.println("  --host <name>           To specify the hostname to send to");
    out.println("  --port <number>         To specify the port to use");
    out.println("  --apikey <apikey>       To specify an API key for authentication");
    out.println("  --token <token>         To specify the access token for a secure connection");
    out.println("  --token-file <path>     Alternative to --token, to specify a file that");
    out.println("                          contains the access token for a secure connection");
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
   * Reads the access token from the tokenFile path
   */
  void readTokenFile() {
    if (tokenFile != null) {
      PrintStream out = verbose ? System.out : System.err;
      try {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(tokenFile));
        String line = bufferedReader.readLine();
        accessToken = line;
      } catch (FileNotFoundException e) {
        out.println("Could not find access token file: " + tokenFile + "\nNo access token will be used");
      } catch (IOException e) {
        out.println("Could not read access token file: " + tokenFile + "\nNo access token will be used");
      }
    }
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
      } else if ("--token".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        accessToken = args[pos].trim().replaceAll("(\r|\n)", "");
      } else if ("--token-file".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        tokenFile = args[pos];
      } else if ("--apikey".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        apikey = args[pos];
      } else if ("--application".equals(arg) || "--app".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        app = args[pos];
      } else if ("--type".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        type = args[pos];
      } else if ("--id".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        id = args[pos];
      } else if ("--filter".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        filters.add(args[pos]);
      } else if ("--value".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        values.add(args[pos]);
      } else if ("--verbose".equals(arg)) {
        verbose = true;
      } else if ("--help".equals(arg)) {
        help = true;
        usage(false);
        return;
      } else {  // unkown argument
        usage(true);
      }
    }
  }

  static List<String> supportedCommands =
    Arrays.asList("list", "read");

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

    // use accessToken if both file and token are provided
    if (tokenFile != null && accessToken != null) {
      tokenFile = null;
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

    if (tokenFile != null) {
      readTokenFile();
    }

    String baseUrl = GatewayUrlGenerator.getBaseUrl(config, hostname, port, !forceNoSSL && apikey != null);
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
    if (apikey != null) {
      get.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, apikey);
    }
    if (accessToken != null) {
      get.setHeader("Authorization", "Bearer " + accessToken);
    }
    try {
      response = client.execute(get);
      if (!checkHttpStatus(response)) {
        return null;
      }
      if (printResponse(response) == null) {
        return null;
      }
      return "OK.";
    } catch (IOException e) {
      System.err.println("Error sending HTTP request: " + e.getMessage());
      return null;
    } finally {
      client.getConnectionManager().shutdown();
    }

  }

  public String printResponse(HttpResponse response) {
    // read the binary value from the HTTP response
    byte[] binaryResponse = Util.readHttpResponse(response);
    if (binaryResponse == null) {
      return null;
    }
    // now make returned value available to user
    System.out.println(new String(binaryResponse, Charsets.UTF_8));
    return "OK.";
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
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
        PrintStream out = (verbose ? System.out : System.err);
        out.println(response.getStatusLine());
        if (accessToken == null) {
          out.println("No access token provided");
        } else {
          Reader reader = null;
          try {
            reader = new InputStreamReader(response.getEntity().getContent());
            String responseError = GSON.fromJson(reader, ErrorMessage.class).getErrorDescription();
            if (responseError != null && !responseError.isEmpty()) {
              out.println(responseError);
            }
          } catch (Exception e) {
            out.println("Unknown unauthorized error");
          }
        }
        return false;
      }
      if (verbose) {
        System.out.println(response.getStatusLine());
      } else {
        System.err.println(response.getStatusLine().getReasonPhrase());
      }
      return false;
    }
    if (verbose) {
      System.out.println(response.getStatusLine());
    }
    return true;
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
   * Error Description from HTTPResponse
   */
  private class ErrorMessage {
    @SerializedName("error_description")
    private String errorDescription;

    public String getErrorDescription() {
      return errorDescription;
    }
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


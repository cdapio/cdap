package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.UsageException;
import com.continuuity.gateway.util.Util;
import com.google.common.collect.Maps;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;

/**
 * This is a command line tool to send events to the data fabric using Flume
 * <ul>
 * <li>It attempts to be smart and determine the port of the Flume
 * collector auto-magically. If that fails, the user can give hints
 * via the --port argument</li>
 * <li>The headers of the event are given on the command line as strings</li>
 * <li>The body can be specified on command line or as a binary file.</li>
 * </ul>
 */
public class FlumeClient {

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
    .getLogger(FlumeClient.class);

  /**
   * for debugging. should only be set to true in unit tests.
   * when true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  boolean verbose = false;       // for debug output
  boolean help = false;          // whether --help was there
  int port = -1;                 // the Flume port of the gateway
  String hostname = null;        // the hostname of the gateway
  String apikey = null;           // the api key for authentication.
  String body = null;            // the body of the event as a String
  String bodyFile = null;        // the file containing the body in binary form
  String destination = null;     // the destination stream
  Map<String, String> headers = Maps.newHashMap(); // to accumulate all headers

  /**
   * Print the usage statement and return null (or empty string if this is not
   * an error case). See getValue() for an explanation of the return type.
   *
   * @param error indicates whether this was invoked as the result of an error
   * @throws UsageException in case of error
   */
  void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = "flume-client";
    if (System.getProperty("script") != null) {
      name = System.getProperty("script").replaceAll("[./]", "");
    }
    out.println("Usage: ");
    out.println("  " + name +
                  " --stream <id> --body <value> [ <option> ... ]");
    out.println("Options:");
    out.println("  --host <name>           To specify the hostname to send to");
    out.println("  --port <number>         To specify the port to use");
    out.println("  --apikey <apikey>       To specify an API key for authentication");
    out.println("  --stream <id>           To specify the destination event stream of the");
    out.println("                          <stream>.");
    out.println("  --header <name> <value> To specify a header for the event to send. Can");
    out.println("                          be used multiple times");
    out.println("  --body <value>          To specify the body of the event as a string");
    out.println("  --body-file <path>      Alternative to --body, to specify a file that");
    out.println("                          contains the binary body of the event");
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
    }
    // go through all the arguments
    for (int pos = 0; pos < args.length; pos++) {
      String arg = args[pos];
      if ("--port".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        try {
          port = Integer.valueOf(args[pos]);
        } catch (NumberFormatException e) {
          usage(true);
        }
      } else if ("--host".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        hostname = args[pos];
      } else if ("--apikey".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        apikey = args[pos];
      } else if ("--stream".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        destination = args[pos];
      } else if ("--header".equals(arg)) {
        if (pos + 2 >= args.length) {
          usage(true);
        }
        headers.put(args[++pos], args[++pos]);
      } else if ("--body".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        body = args[pos];
      } else if ("--body-file".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        bodyFile = args[pos];
      } else if ("--help".equals(arg)) {
        usage(false);
        help = true;
        return;
      } else if ("--verbose".equals(arg)) {
        verbose = true;
      } else {  // unkown argument
        usage(true);
      }
    }
  }

  void validateArguments(String[] args) {
    // first parse command arguments
    parseArguments(args);
    if (help) {
      return;
    }
    // verify that either --body or --body-file is given
    if (body != null && bodyFile != null) {
      usage("Either --body or --body-file must be specified.");
    }
    // verify that a destination was given
    if (destination == null) {
      usage("A destination stream must be specified.");
    }
  }

  /**
   * read the body of the event in binary form, either from
   * --body or --body-file.
   */
  byte[] readBody() {
    if (body != null) {
      return body.getBytes();
    } else if (bodyFile != null) {
      return Util.readBinaryFile(bodyFile);
    } else {
      return null;
    }
  }

  /**
   * This is actually the main method, but in order to make it testable,
   * instead of exiting in case of error it returns null, whereas in case
   * of success it returns the retrieved value as shown on the console.
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

    // determine the flume port for the GET request
    if (port == -1) {
      port = config.getInt(Constants.Gateway.STREAM_FLUME_PORT,
                           Constants.Gateway.DEFAULT_STREAM_FLUME_PORT);
    }
    if (port == -1) {
      System.err.println("Can't figure out the URL to send to. " +
                           "Please use --port to specify.");
      return null;
    }
    // determine the gateway host
    if (hostname == null) {
      hostname = "localhost";
    }
    if (verbose) {
      System.out.println("Using flume port: " + hostname + ":" + port);
    }

    // get the body as a byte array
    byte[] binaryBody = readBody();
    if (binaryBody == null) {
      System.err.println("Cannot send an event without body. " +
                           "Please use --body or --body-file to specify the body.");
      return null;
    }

    // create a flume event
    SimpleEvent event = new SimpleEvent();
    event.setBody(binaryBody);
    event.getHeaders().put(Constants.Gateway.HEADER_DESTINATION_STREAM, destination);
    for (String header : headers.keySet()) {
      event.getHeaders().put(header, headers.get(header));
    }
    // add apikey if specified
    if (apikey != null) {
      event.getHeaders().put(Constants.Gateway.CONTINUUITY_API_KEY, apikey);
    }

    // event is now fully constructed, ready to send
    RpcClient client = RpcClientFactory.getDefaultInstance(hostname, port, 1);
    try {
      client.append(event);
    } catch (EventDeliveryException e) {
      client.close();
      System.err.println("Error sending flume event: " + e.getMessage());
      return null;
    }
    client.close();
    return "OK.";
  }

  public String execute(String[] args, CConfiguration config) {
    try {
      return execute0(args, config);
    } catch (UsageException e) {
      if (debug) { // this is mainly for debugging the unit test
        System.err.println("Exception for arguments: " +
                             Arrays.toString(args) + ". Exception: " + e);
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
    // create an event client and run it with the given arguments
    FlumeClient instance = new FlumeClient();
    String value = instance.execute(args, config);
    // exit with error in case fails
    if (value == null) {
      System.exit(1);
    }
  }
}





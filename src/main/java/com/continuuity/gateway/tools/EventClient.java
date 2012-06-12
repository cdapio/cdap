package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.util.Util;
import com.google.common.collect.Maps;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;

/**
 * This is a command line tool to send events to the data fabric using REST
 * <ul>
 * <li>It attempts to be smart and determine the URL of the REST
 * collector auto-magically. If that fails, the user can give hints
 * via the --connector and --base arguments</li>
 * <li>The headers of the event are given on the command line as strings</li>
 * <li>The body can be specified on command line or as a binary file.</li>
 * </ul>
 */
public class EventClient {

  private static final Logger LOG = LoggerFactory
      .getLogger(EventClient.class);

  /**
   * for debugging. should only be set to true in unit tests.
   * when true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  boolean verbose = false;       // for debug output
  boolean help = false;          // whether --help was there
  String baseUrl = null;         // the base url for HTTP requests
  String hostname = null;        // the hostname of the gateway
  String connector = null;       // the name of the rest accessor
  String body = null;            // the body of the event as a String
  String bodyFile = null;        // the file that contains the body in binary form
  String destination = null;     // the destination stream
  Map<String, String> headers = Maps.newHashMap(); // to accumulate all the headers for the event


  /**
   * Print the usage statement and return null (or empty string if this is not an error case).
   * See getValue() for an explanation of the return type.
   *
   * @param error indicates whether this was invoked as the result of an error
   * @throws IllegalArgumentException in case of error, an empty string in case of success
   */
  void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    out.println("Usage: EventClient <option> ... with");
    out.println("  --base <url>            To specify the port to use");
    out.println("  --host <name>           To specify the hostname to send to");
    out.println("  --connector <name>      To specify the name of the rest collector");
    out.println("  --stream <name>         To specify the destination event stream");
    out.println("  --header <name> <value> To specify a header for the event to send. Can be used multiple times");
    out.println("  --body <value>          To specify the body of the event as a string");
    out.println("  --body-file <path>      To specify a file containing the binary body of the event");
    out.println("  --verbose               To see more verbose output");
    out.println("  --help                  To print this message");
    if (error) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Print an error message followed by the usage statement
   * @param errorMessage the error message
   */
  void usage(String errorMessage) {
    if (errorMessage != null) System.err.println("Error: " + errorMessage);
    usage(true);
  }

  /**
   * Parse the command line arguments
   */
  void parseArguments(String[] args) {
    if (args.length == 0) usage(true);
    if ("--help".equals(args[0])) {
      usage(false);
      help = true;
      return;
    }
    // go through all the arguments
    for (int pos = 0; pos < args.length; pos++) {
      String arg = args[pos];
      if ("--base".equals(arg)) {
        if (++pos >= args.length) usage(true);
        baseUrl = args[pos];
      } else if ("--host".equals(arg)) {
        if (++pos >= args.length) usage(true);
        hostname = args[pos];
      } else if ("--connector".equals(arg)) {
        if (++pos >= args.length) usage(true);
        connector = args[pos];
      } else if ("--stream".equals(arg)) {
        if (++pos >= args.length) usage(true);
        destination = args[pos];
      } else if ("--header".equals(arg)) {
        if (pos + 2 >= args.length) usage(true);
        headers.put(args[++pos], args[++pos]);
      } else if ("--body".equals(arg)) {
        if (++pos >= args.length) usage(true);
        body = args[pos];
      } else if ("--body-file".equals(arg)) {
        if (++pos >= args.length) usage(true);
        bodyFile = args[pos];
      } else if ("--help".equals(arg)) {
        usage(false);
        help = true;
        return;
      } else {  // unkown argument
        usage(true);
      }
    }
  }

  void validateArguments(String[] args) {
    // first parse command arguments
    parseArguments(args);
    if (help) return;
    // verify that either --body or --body-file is given
    if (body != null && bodyFile != null) usage("Either --body or --body-file must be specified.");
    // verify that a destination was given
    if (destination == null) usage("A destination stream must be specified.");
    // verify that only one hint is given for the URL
    if (hostname != null && baseUrl != null) usage("Only one of --host or --base may be specified.");
    if (connector != null && baseUrl != null) usage("Only one of --connector or --base may be specified.");
  }

  /**
   * read the body of the event in binary form, either from
   * --body or --body-file
   */
  byte[] readBody() {
    if (body != null) {
      return body.getBytes();
    }
    else if (bodyFile != null) {
      return Util.readBinaryFile(bodyFile);
    }
    else {
      return null;
    }
  }

  /**
   * This is actually the main method, but in order to make it testable, instead of exiting in case
   * of error it returns null, whereas in case of success it returns the retrieved value as shown
   * on the console.
   *
   * @param args   the command line arguments of the main method
   * @param config The configuration of the gateway
   * @return null in case of error, an string representing the retrieved value in case of success
   */
  public String execute0(String[] args, CConfiguration config) {
    // parse and validate arguments
    validateArguments(args);
    if (help) return "";

    // determine the base url for the GET request
    if (baseUrl == null)
      baseUrl = Util.findBaseUrl(config, RestCollector.class, connector, hostname);
    if (baseUrl == null) {
      System.err.println("Can't figure out the URL to send to. Please use --base or --connector to specify.");
      return null;
    } else {
      if (verbose) System.out.println("Using base URL: " + baseUrl);
    }

    // build the full URI for the request and validate it
    String requestUrl = baseUrl + destination;
    URI uri = null;
    try {
      uri = new URI(requestUrl);
    } catch (URISyntaxException e) {
      // this can only happen if the --host, --base, or --stream are not valid for a URL
      System.err.println("Invalid request URI '" + requestUrl
          + "'. Check the validity of --host, --base or --stream arguments.");
      return null;
    }

    // get the body as a byte array
    byte[] binaryBody = readBody();
    if (body == null) {
      System.err.println("Cannot send an event without body. Please use --body or --body-file to specify the body.");
      return null;
    }

    // create an HttpPost
    HttpPost post = new HttpPost(uri);
    for (String header : headers.keySet()) {
      post.setHeader(header, headers.get(header));
    }
    post.setEntity(new ByteArrayEntity(binaryBody));
    // post is now fully constructed, ready to send

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    try {
      response = client.execute(post);
      client.getConnectionManager().shutdown();
    } catch (IOException e) {
      System.err.println("Error sending HTTP request: " + e.getMessage());
      return null;
    }
    // show the HTTP status and verify it was successful
    System.out.println(response.getStatusLine());
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      return null;
    }
    return "OK.";
  }

  public String execute(String[] args, CConfiguration config) {
    try {
      return execute0(args, config);
    } catch (IllegalArgumentException e) {
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
    EventClient instance = new EventClient();
    String value = instance.execute(args, CConfiguration.create());
    if (value == null) {
      System.exit(1);
    }
  }
}
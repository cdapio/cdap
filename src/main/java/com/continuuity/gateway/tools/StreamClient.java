package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.Copyright;
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
import java.util.List;
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
public class StreamClient {

  private static final Logger LOG = LoggerFactory
      .getLogger(StreamClient.class);

  /**
   * for debugging. should only be set to true in unit tests.
   * when true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  String command = null;         // the command to run
  boolean verbose = false;       // for debug output
  boolean help = false;          // whether --help was there
  boolean hex = false;           // whether body is in hex noatation
  boolean urlenc = false;        // whether body is in url encoding
  String baseUrl = null;         // the base url for HTTP requests
  String hostname = null;        // the hostname of the gateway
  String connector = null;       // the name of the rest collector
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
    String name = this.getClass().getSimpleName();
    Copyright.print(out);
    out.println("Usage: ");
    out.println("  " + name + " send --stream <name> --body <value> [ <option> ... ]");
    out.println("Options:");
    out.println("  --base <url>            To specify the base URL to use");
    out.println("  --host <name>           To specify the hostname to send to");
    out.println("  --connector <name>      To specify the name of the rest collector");
    out.println("  --stream <name>         To specify the destination event stream of the");
    out.println("                          form <flow> or <flow>/<stream>.");
    out.println("  --header <name> <value> To specify a header for the event to send. Can");
    out.println("                          be used multiple times");
    out.println("  --body <value>          To specify the body of the event as a string");
    out.println("  --body-file <path>      Alternative to --body, to specify a file that");
    out.println("                          contains the binary body of the event");
    out.println("  --hex                   To specify hexadecimal encoding for --body");
    out.println("  --url                   To specify url encoding for --body");
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
    } else {
      command = args[0];
    }
    // go through all the arguments
    for (int pos = 1; pos < args.length; pos++) {
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
      } else if ("--hex".equals(arg)) {
        hex = true;
      } else if ("--url".equals(arg)) {
        urlenc = true;
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

  static List<String> supportedCommands = Arrays.asList("send");

  void validateArguments(String[] args) {
    // first parse command arguments
    parseArguments(args);
    if (help) return;
    // first validate the command
    if (!supportedCommands.contains(command)) usage("Unsupported command '" + command + "'.");
    // verify that either --body or --body-file is given
    if (body != null && bodyFile != null) usage("Either --body or --body-file must be specified.");
    // verify that a destination was given
    if (destination == null) usage("A destination stream must be specified.");
    // verify that only one hint is given for the URL
    if (hostname != null && baseUrl != null) usage("Only one of --host or --base may be specified.");
    if (connector != null && baseUrl != null) usage("Only one of --connector or --base may be specified.");
    // verify that only one encoding is given for the body
    if (hex && urlenc) usage("Only one of --hex or --url may be specified");
    if (bodyFile != null && (hex || urlenc)) usage("Options --hex and --url are incompatible with --body-file (binary input)");
  }

  /**
   * read the body of the event in binary form, either from
   * --body or --body-file
   */
  byte[] readBody() {
    if (body != null) {
      if (urlenc)
        return Util.urlDecode(body);
      else if (hex)
        return Util.hexValue(body);
      return body.getBytes();
    }
    else if (bodyFile != null)
      return Util.readBinaryFile(bodyFile);
    return null;
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
    if (binaryBody == null) {
      System.err.println("Cannot send an event without body. Please use --body or --body-file to specify the body.");
      return null;
    }

    // create an HttpPost
    HttpPost post = new HttpPost(uri);
    for (String header : headers.keySet()) {
      post.setHeader(destination + "." + header, headers.get(header));

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
    if (!checkHttpStatus(response)) return null;
    return "OK.";
  }

  /**
   * Check whether the Http return code is positive. If not, print the error message
   * and return false. Otherwise, if verbose is on, print the response status line.
   * @param response the HTTP response
   * @return whether the response indicates success
   */
  boolean checkHttpStatus(HttpResponse response) {
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      if (verbose)
        System.out.println(response.getStatusLine());
      else
        System.err.println(response.getStatusLine().getReasonPhrase());
      return false;
    }
    if (verbose)
      System.out.println(response.getStatusLine());
    return true;
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
    // create a config and load the gateway properties
    CConfiguration config = CConfiguration.create();
    config.addResource("continuuity-gateway.xml");
    // create an event client and run it with the given arguments
    StreamClient instance = new StreamClient();
    String value = instance.execute(args, config);
    // exit with error in case fails
    if (value == null) System.exit(1);
  }
}
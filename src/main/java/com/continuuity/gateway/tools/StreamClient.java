package com.continuuity.gateway.tools;

import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.common.collect.AllCollector;
import com.continuuity.common.collect.Collector;
import com.continuuity.common.collect.FirstNCollector;
import com.continuuity.common.collect.LastNCollector;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.Copyright;
import com.continuuity.common.utils.UsageException;
import com.continuuity.flow.flowlet.internal.EventBuilder;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.util.Util;
import com.google.common.collect.Maps;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
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

@SuppressWarnings("unused")
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
  String bodyFile = null;        // the file containing the body in binary form
  String destination = null;     // the destination stream
  String consumer = null;        // consumer group id to fetch from the stream
  boolean all = false;           // whether to view all events in the stream
  Integer last = null;           // to view the N last events in the stream
  Integer first = null;          // to view the N first events in the stream
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
    String name = System.getProperty("script").replace(".","/").replace("/","");
    Copyright.print(out);
    out.println("Usage: ");
    out.println("  " + name +
        " send --stream <name> --body <value> [ <option> ... ]");
    out.println("  " + name + " id --stream <name> [ <option> ... ]");
    out.println("  " + name +
        " fetch --stream <name> --consumer <id> [ <option> ... ]");
    out.println("  " + name + " view --stream <name> [ <option> ... ]");
    out.println("  " + name + " info --stream <name> [ <option> ... ]");
    out.println("Options:");
    out.println("  --base <url>            To specify the base URL to use");
    out.println("  --host <name>           To specify the hostname to send to");
    out.println("  --connector <name>      " +
        "To specify the name of the rest collector");
    out.println("  --stream <name>         " +
        "To specify the destination event stream of the");
    out.println("                          form <flow> or <flow>/<stream>.");
    out.println("  --header <name> <value> " +
        "To specify a header for the event to send. Can");
    out.println("                          be used multiple times");
    out.println("  --body <value>          " +
        "To specify the body of the event as a string");
    out.println("  --body-file <path>      " +
        "Alternative to --body, to specify a file that");
    out.println("                          " +
        "contains the binary body of the event");
    out.println("  --hex                   " +
        "To specify hexadecimal encoding for --body");
    out.println("  --url                   To specify url encoding for --body");
    out.println("  --id <group id>         " +
        "To specify a consumer group id for the stream, as ");
    out.println("                          obtained by " + name + " id");
    out.println("  --all                   To view the entire stream.");
    out.println("  --first <number>        " +
        "To view the first N events in the stream. Default ");
    out.println("                          for view is --first 10.");
    out.println("  --last <number>         " +
        "To view the last N events in the stream.");
    out.println("  --verbose               To see more verbose output");
    out.println("  --help                  To print this message");
    if (error) {
      throw new UsageException();
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
      } else if ("--all".equals(arg)) {
        all = true;
      } else if ("--first".equals(arg)) {
        if (++pos >= args.length) usage(true);
        try {
          first = Integer.valueOf(args[pos]);
        } catch (NumberFormatException e) {
          usage(true);
        }
      } else if ("--last".equals(arg)) {
        if (++pos >= args.length) usage(true);
        try {
          last = Integer.valueOf(args[pos]);
        } catch (NumberFormatException e) {
          usage(true);
        }
      } else if ("--id".equals(arg)) {
        if (++pos >= args.length) usage(true);
        try {
          consumer = args[pos];
          // validate that it is a number
          Long.valueOf(consumer);
        } catch (NumberFormatException e) {
          usage(true);
        }
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

  static List<String> supportedCommands =
      Arrays.asList("send", "id", "fetch", "view", "info");

  void validateArguments(String[] args) {
    // first parse command arguments
    parseArguments(args);
    if (help) return;
    // first validate the command
    if (!supportedCommands.contains(command))
      usage("Unsupported command '" + command + "'.");
    // verify that either --body or --body-file is given
    if ("send".equals(command) && body != null && bodyFile != null)
      usage("Either --body or --body-file must be specified.");
    // verify that a destination was given
    if (destination == null) usage("A destination stream must be specified.");
    // verify that only one hint is given for the URL
    if (hostname != null && baseUrl != null)
      usage("Only one of --host or --base may be specified.");
    if (connector != null && baseUrl != null)
      usage("Only one of --connector or --base may be specified.");
    // verify that only one encoding is given for the body
    if (hex && urlenc) usage("Only one of --hex or --url may be specified");
    if (bodyFile != null && (hex || urlenc))
      usage("Options --hex and --url are incompatible with --body-file " +
          "(binary input)");
    // make sure that fetch command has a consumer id
    if ("fetch".equals(command) && consumer == null)
      usage("--consumer must be specified for fetch");
    // make sure that view command does not have contradicting options
    if ("view".equals(command)) {
      if ((all && first != null) || (all && last != null) ||
          (last != null && first != null))
        usage("Only one of --all, --first or --last may be specified");
      if (first != null && first < 1) usage("--first must be at least 1");
      if (last != null && last < 1) usage("--last must be at least 1");
    }
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

  /*
   * return the resulting value to the use, following arguments
   */
  String writeBody(byte[] binaryBody) {
    // was a file specified to write to?
    if (bodyFile != null) {
      try {
        FileOutputStream out = new FileOutputStream(bodyFile);
        out.write(binaryBody);
        out.close();
        if (verbose) System.out.println(binaryBody.length +
            " bytes written to file " + bodyFile + ".");
        return binaryBody.length + " bytes written to file";
      } catch (IOException e) {
        System.err.println(
            "Error writing to file " + bodyFile + ": " + e.getMessage());
        return null;
      } }
    String body;
    // was hex encoding requested?
    if (hex)
      body = Util.toHex(binaryBody);
    // or was URl encoding specified?
    else if (urlenc)
      body = Util.urlEncode(binaryBody);
    // by default, assume the same encoding for the value as for the key
    else
      body = new String(binaryBody);

    if (verbose) System.out.println(
        "Body[" + binaryBody.length + " bytes]: " + body);
    else System.out.println(body);
    return body;
  }

  /**
   * This is actually the main method, but in order to make it testable,
   * instead of exiting in case of error it returns null, whereas in case of
   * success it returns the retrieved value as shown on the console.
   *
   * @param args   the command line arguments of the main method
   * @param config The configuration of the gateway
   * @return null in case of error, an string representing the retrieved value
   * in case of success
   */
  public String execute0(String[] args, CConfiguration config) {
    // parse and validate arguments
    validateArguments(args);
    if (help) return "";

    // determine the base url for the GET request
    if (baseUrl == null) baseUrl =
        Util.findBaseUrl(config, RestCollector.class, connector, hostname);
    if (baseUrl == null) {
      System.err.println("Can't figure out the URL to send to. " +
          "Please use --base or --connector to specify.");
      return null;
    } else {
      if (verbose) System.out.println("Using base URL: " + baseUrl);
    }

    // build the full URI for the request and validate it
    String requestUrl = baseUrl + destination;

    if ("send".equals(command)) {
      // get the body as a byte array
      byte[] binaryBody = readBody();
      if (binaryBody == null) {
        System.err.println("Cannot send an event without body. " +
            "Please use --body or --body-file to specify the body.");
        return null;
      }

      // create an HttpPost
      HttpPost post = new HttpPost(requestUrl);
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

    else if ("id".equals(command)) {
      String id = getConsumerId(requestUrl);
      if (id != null) {
        System.out.println(id);
        return "OK.";
      } else {
        return null;
      }
    }

    else if ("info".equals(command)) {
      String info = getQueueInfo(requestUrl);
      if (info != null) {
        System.out.println(info);
        return "OK.";
      } else {
        return null;
      }
    }

    else if ("fetch".equals(command)) {
      Event event;
      try {
        event = fetchOne(requestUrl, consumer);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        return null;
      }
      // print all the headers
      for (String name : event.getHeaders().keySet()) {
        // unless --verbose was given, we suppress continuuity headers
        if (!verbose && Constants.isContinuuityHeader(name))
          continue;
        System.out.println(name + ": " + event.getHeader(name));
      }
      // and finally write out the body
      return writeBody(event.getBody());
    }

    else if ("view".equals(command)) {
      if (consumer == null) {
        // prepare for HTTP
        HttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(requestUrl + "?q=newConsumer");
        HttpResponse response;
        try {
          response = client.execute(get);
          client.getConnectionManager().shutdown();
        } catch (IOException e) {
          System.err.println("Error sending HTTP request: " + e.getMessage());
          return null;
        }
        if (!checkHttpStatus(response, HttpStatus.SC_CREATED)) return null;
        // read the binary value from the HTTP response
        byte[] binaryValue = Util.readHttpResponse(response);
        if (binaryValue == null) {
          System.err.println("Unexpected response without body.");
          return null;
        }
        consumer = new String(binaryValue);
      }
      Collector<Event> collector =
          all ? new AllCollector<Event>(Event.class) :
              first != null ? new FirstNCollector<Event>(first, Event.class) :
                  last != null ? new LastNCollector<Event>(last, Event.class) :
                      new FirstNCollector<Event>(10, Event.class);
      try {
        Event[] events =
            fetchAll(requestUrl + "?q=dequeue", consumer, collector);
        return printEvents(events);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        return null;
      }
    }
    return null;
  }

  /**
   * This implements --id, it obtains a new consumer group id
   * @param requestUrl the base url with the stream added to it
   * @return the consumer group id returned by the gateway
   */
  String getConsumerId(String requestUrl) {
    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(requestUrl + "?q=newConsumer");
    HttpResponse response;
    try {
      response = client.execute(get);
      client.getConnectionManager().shutdown();
    } catch (IOException e) {
      System.err.println("Error sending HTTP request: " + e.getMessage());
      return null;
    }
    // this call does not respond with 200 OK, but with 201 Created
    if (!checkHttpStatus(response, HttpStatus.SC_CREATED)) return null;

    // read the binary value from the HTTP response
    byte[] binaryValue = Util.readHttpResponse(response);
    if (binaryValue == null) {
      System.err.println("Unexpected response without body.");
      return null;
    }
    return new String(binaryValue);
  }

  /**
   * This implements --info, it retrieves queue meta information
   * @param requestUrl the base url with the stream added to it
   * @return the consumer group id returned by the gateway,
   * null in case of error
   */
  String getQueueInfo(String requestUrl) {
    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(requestUrl);
    HttpResponse response;
    try {
      response = client.execute(get);
      client.getConnectionManager().shutdown();
    } catch (IOException e) {
      System.err.println("Error sending HTTP request: " + e.getMessage());
      return null;
    }
    // this call does not respond with 200 OK, but with 201 Created
    if (!checkHttpStatus(response)) return null;

    // read the binary value from the HTTP response
    byte[] binaryValue = Util.readHttpResponse(response);
    if (binaryValue == null) {
      System.err.println("Unexpected response without body.");
      return null;
    }
    return new String(binaryValue);
  }

  /**
   * Helper method for --view, given a request URL already constructed, a
   * consumer ID, and an event collector, it iterates over events from the
   * stream until the stream is empty or the collector indicates to stop.
   * @param uri The request URI including the stream name without the query
   * @param consumer the consumer group id, as previously returned by
   *                 getConsumerId()
   * @param collector a collector for the events in the stream
   * @return all events collected
   * @throws Exception if something goes wrong
   */
  Event[] fetchAll(String uri, String consumer, Collector<Event> collector)
      throws Exception {
    while (true) {
      Event event = fetchOne(uri, consumer);
      if (event == null) return collector.finish();
      boolean collectMore = collector.addElement(event);
      if (!collectMore) return collector.finish();
    }
  }

  /**
   * Helper method for --view, given a request URL already constructed and a
   * consumer ID, it fetches (dequeues) one event from the stream.
   * @param uri The request URI including the stream name without the query
   * @param consumer the consumer group id, as previously returned by
   *                 getConsumerId()
   * @return the event that was fetched, or null if the stream is empty
   * @throws Exception if something goes wrong
   */
  Event fetchOne(String uri, String consumer) throws Exception {
    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(uri);
    get.addHeader(Constants.HEADER_STREAM_CONSUMER, consumer);
    HttpResponse response;
    try {
      response = client.execute(get);
      client.getConnectionManager().shutdown();
    } catch (IOException e) {
      throw new Exception("Error sending HTTP request.", e);
    }
    // we expect either OK for an event, or NO_CONTENT for end of stream
    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NO_CONTENT)
      return null;
    // check that the response is OK
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
      throw new Exception(
          "HTTP request unsuccessful: " + response.getStatusLine());
    // read the binary value from the HTTP response
    byte[] binaryValue = Util.readHttpResponse(response);
    if (binaryValue == null)
      throw new Exception("Unexpected response without body.");

    // collect all the headers
    EventBuilder builder = new EventBuilder();
    builder.setBody(binaryValue);
    for (org.apache.http.Header header : response.getAllHeaders()) {
      String name = header.getName();
      // ignore common HTTP headers. All the headers of the actual
      // event are transmitted with the stream name as a prefix
      if (name.startsWith(destination)) {
        String actualName = name.substring(destination.length() + 1);
        builder.setHeader(actualName, header.getValue());
      }
    }
    return builder.create();
  }

  /**
   * Helper for --view. Prints all the collected events to stdout. This can be
   * improved to use better formatting, shortening, etc.
   * @param events An array of events
   * @return a String indicating how many events were printed
   */
  String printEvents(Event[] events) {
    System.out.println(events.length + " events: ");
    for (Event event : events) {
      String sep = "";
      for (String name : event.getHeaders().keySet()) {
        // unless --verbose was given, we suppress continuuity headers
        if (!verbose && Constants.isContinuuityHeader(name))
          continue;
        System.out.print(sep + name + "=" + event.getHeader(name));
        sep = ", ";
      }
      System.out.print(": ");
      if (hex) System.out.print(Util.toHex(event.getBody()));
      else if (urlenc) System.out.print(Util.urlEncode(event.getBody()));
      else System.out.print(new String(event.getBody()));
      System.out.println();
    }
    return events.length + " events.";
  }

  /**
   * Check whether the Http return code is 200 OK. If not, print the error
   * message and return false. Otherwise, if verbose is on, print the response
   * status line.
   * @param response the HTTP response
   * @return whether the response is OK
   */
  boolean checkHttpStatus(HttpResponse response) {
    return checkHttpStatus(response, HttpStatus.SC_OK);
  }

  /**
   * Check whether the Http return code is as expected. If not, print the error
   * message and return false. Otherwise, if verbose is on, print the response
   * status line.
   * @param response the HTTP response
   * @param expected the expected HTTP status code
   * @return whether the response is as expected
   */
  boolean checkHttpStatus(HttpResponse response, int expected) {
    return checkHttpStatus(response, Collections.singletonList(expected));
  }

  /**
   * Check whether the Http return code is as expected. If not, print the
   * status message and return false. Otherwise, if verbose is on, print the
   * response status line.
   * @param response the HTTP response
   * @param expected the list of expected HTTP status codes
   * @return whether the response is as expected
   */
  boolean checkHttpStatus(HttpResponse response, List<Integer> expected) {
    if (!expected.contains(response.getStatusLine().getStatusCode())) {
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
    StreamClient instance = new StreamClient();
    String value = instance.execute(args, config);
    // exit with error in case fails
    if (value == null) System.exit(1);
  }
}

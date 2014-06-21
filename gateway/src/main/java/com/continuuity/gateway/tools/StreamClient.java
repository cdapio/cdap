package com.continuuity.gateway.tools;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.common.collect.AllCollector;
import com.continuuity.common.collect.Collector;
import com.continuuity.common.collect.FirstNCollector;
import com.continuuity.common.collect.LastNCollector;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.stream.DefaultStreamEvent;
import com.continuuity.common.utils.UsageException;
import com.continuuity.gateway.util.Util;
import com.continuuity.internal.app.verification.StreamVerification;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.log4j.Level;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
public class StreamClient extends ClientToolBase {

  private static final Gson GSON = new Gson();
  private static final String HEX_OPTION = "hex";
  private static final String STREAM_OPTION = "stream";
  private static final String BODY_OPTION = "body";
  private static final String BODY_FILE_OPTION = "body-file";
  private static final String URL_OPTION = "url";
  private static final String GROUP_OPTION = "group";
  private static final String ALL_OPTION = "all";
  private static final String FIRST_OPTION = "first";
  private static final String LAST_OPTION = "last";
  private static final String TTL_OPTION = "ttl";
  private static final String NAME = "stream-client";

  static {
    // this turns off all logging but we don't need that for a cmdline tool
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.OFF);
  }

  /**
   * for debugging. should only be set to true in unit tests.
   * when true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  String command = null;         // the command to run
  boolean hex = false;           // whether body is in hex noatation
  boolean urlenc = false;        // whether body is in url encoding
  String body = null;            // the body of the event as a String
  String bodyFile = null;        // the file containing the body in binary form
  String destination = null;     // the destination stream (stream id)
  String consumer = null;        // consumer group id to fetch from the stream
  boolean all = false;           // whether to view all events in the stream
  Integer last = null;           // to view the N last events in the stream
  Integer first = null;          // to view the N first events in the stream
  Long ttl = null;               // Stream TTL
  Map<String, String> headers = Maps.newHashMap(); // to accumulate all headers

  boolean forceNoSSL = false;

  public StreamClient() {
    super("stream-client");
  }

  public StreamClient(String toolName) {
    super(toolName);
    buildOptions();
  }

  public StreamClient disallowSSL() {
    this.forceNoSSL = true;
    return this;
  }

  @Override
  public void buildOptions() {
    // build the default options
    super.buildOptions();
    options.addOption(null, STREAM_OPTION, true, "To specify the destination event stream of the" +
                      "form <flow> or <flow>/<stream>.");
    options.addOption(null, BODY_OPTION, true, "To specify the body of the event as a string");
    options.addOption(null, BODY_FILE_OPTION, true, "Alternative to --body, to specify a file that" +
                      "contains the binary body of the event");
    options.addOption(null, HEX_OPTION, false, "To specify hexadecimal encoding for --body");
    options.addOption(null, URL_OPTION, false, "To specify url encoding for --body");
    options.addOption(null, GROUP_OPTION, true, "To specify a consumer group id for the stream, as " +
                      "obtained by " + NAME + " group command");
    options.addOption(null, ALL_OPTION, false, "To view the entire stream");
    options.addOption(null, FIRST_OPTION, true, "To view the first N events in the stream. " +
                      "for view is --first 10");
    options.addOption(null, LAST_OPTION, true, "To view the last N events in the stream");
    options.addOption(null, TTL_OPTION, true, "To set the TTL for the stream in seconds");
  }

  @Override
  public void printUsage(boolean error) {
    PrintStream out = error ? System.err : System.out;
    out.println("Usage:\n");
    out.println("\t" + getToolName() + " create --stream <id>");
    out.println("\t" + getToolName() + " send --stream <id> --body <value> [ <option> ... ]");
    out.println("\t" + getToolName() + " group --stream <id> [ <option> ... ]");
    out.println("\t" + getToolName() + " fetch --stream <id> --group <id> [ <option> ... ]");
    out.println("\t" + getToolName() + " view --stream <id> [ <option> ... ]");
    out.println("\t" + getToolName() + " info --stream <id> [ <option> ... ]");
    out.println("\t" + getToolName() + " truncate --stream <id>");
    out.println("\t" + getToolName() + " config --stream <id> [ <option> ... ]\n");
    super.printUsage(error);
  }

//  /**
//   * Print the usage statement and return null (or empty string if this is not
//   * an error case). See getValue() for an explanation of the return type.
//   *
//   * @param error indicates whether this was invoked as the result of an error
//   * @throws UsageException in case of error
//   */
//  void usage(boolean error) {
//    PrintStream out = (error ? System.err : System.out);
//    String name = "stream-client";
//    if (System.getProperty("script") != null) {
//      name = System.getProperty("script").replaceAll("[./]", "");
//    }
//    out.println("Usage: ");
//    out.println("  " + name + " create --stream <id>");
//    out.println("  " + name + " send --stream <id> --body <value> [ <option> ... ]");
//    out.println("  " + name + " group --stream <id> [ <option> ... ]");
//    out.println("  " + name + " fetch --stream <id> --group <id> [ <option> ... ]");
//    out.println("  " + name + " view --stream <id> [ <option> ... ]");
//    out.println("  " + name + " info --stream <id> [ <option> ... ]");
//    out.println("  " + name + " truncate --stream <id>");
//    out.println("  " + name + " config --stream <id> [ <option> ... ]");
//    out.println("Options:");
//    out.println("  --stream <id>           To specify the destination event stream of the");
//    out.println("                          form <flow> or <flow>/<stream>.");
//    out.println("  --header <name> <value> To specify a header for the event to send. Can");
//    out.println("                          be used multiple times");
//    out.println("  --body <value>          To specify the body of the event as a string");
//    out.println("  --body-file <path>      Alternative to --body, to specify a file that");
//    out.println("                          contains the binary body of the event");
//    out.println("  --hex                   To specify hexadecimal encoding for --body");
//    out.println("  --url                   To specify url encoding for --body");
//    out.println("  --group <id>            To specify a consumer group id for the stream, as ");
//    out.println("                          obtained by " + name + " group command ");
//    out.println("  --all                   To view the entire stream.");
//    out.println("  --first <number>        To view the first N events in the stream. Default ");
//    out.println("                          for view is --first 10.");
//    out.println("  --last <number>         To view the last N events in the stream.");
//    out.println("  --ttl <number>          To set the TTL for the stream in seconds.");
//    if (error) {
//      throw new UsageException();
//    }
//  }
//
//  /**
//   * Print an error message followed by the usage statement.
//   *
//   * @param errorMessage the error message
//   */
//  void usage(String errorMessage) {
//    if (errorMessage != null) {
//      System.err.println("Error: " + errorMessage);
//    }
//    usage(true);
//  }

  public boolean parseArguments(String[] args) {
    // parse generic args first
    CommandLineParser parser = new GnuParser();
    // Check all the options of the command line
    try {
      command = args[0];
      // check for header arguments
      for (int pos = 1; pos < args.length; ++pos) {
        if ("--header".equals(args[pos])) {
          if (pos + 2 >= args.length) {
            printUsage(true);
          }
          headers.put(args[++pos], args[++pos]);
        }
      }
      CommandLine line = parser.parse(options, args);
      parseBasicArgs(line);
      // returns false if help was passed
      if (help) {
        return false;
      }
      destination = line.hasOption(STREAM_OPTION) ? line.getOptionValue(STREAM_OPTION) : null;
      body = line.hasOption(BODY_OPTION) ? line.getOptionValue(BODY_OPTION) : null;
      bodyFile = line.hasOption(BODY_FILE_OPTION) ? line.getOptionValue(BODY_FILE_OPTION) : null;
      hex = line.hasOption(HEX_OPTION);
      urlenc = line.hasOption(URL_OPTION);
      all = line.hasOption(ALL_OPTION);
      // validate consumer is a numerical value
      if (line.hasOption(GROUP_OPTION)) {
        consumer = line.getOptionValue(GROUP_OPTION);
        try {
          Long.valueOf(consumer);
        } catch (NumberFormatException e) {
          usage("--" + GROUP_OPTION + " must have a long integer argument");
        }
      } else {
        consumer = null;
      }

      try {
        first = parseNumericArg(line, FIRST_OPTION).intValue();
      } catch (NullPointerException e) { }
      try {
        last = parseNumericArg(line, LAST_OPTION).intValue();
      } catch (NullPointerException e) { }
      try {
        ttl = parseNumericArg(line, TTL_OPTION);
      } catch (NullPointerException e) { }

      // TODO Check for extra params
      // expect at least 1 extra arg because of pos arg, the command to run
      if (line.getArgs().length > 1) {
        System.err.println("Found extra args");
        printUsage(true);
      }
    } catch (ParseException e) {
      printUsage(true);
    } catch (IndexOutOfBoundsException e) {
      printUsage(true);
    }
    return true;
  }

//  /**
//   * Parse the command line arguments.
//   */
//  void parseArguments(String[] args) {
//    if (args.length == 0) {
//      usage(true);
//    }
//    if ("--help".equals(args[0])) {
//      usage(false);
//      return;
//    } else {
//      command = args[0];
//    }
//    // go through all the arguments
//    for (int pos = 1; pos < args.length; pos++) {
//      String arg = args[pos];
//      if ("--stream".equals(arg)) {
//        if (++pos >= args.length) {
//          usage(true);
//        }
//        destination = args[pos];
//      } else if ("--header".equals(arg)) {
//        if (pos + 2 >= args.length) {
//          usage(true);
//        }
//        headers.put(args[++pos], args[++pos]);
//      } else if ("--body".equals(arg)) {
//        if (++pos >= args.length) {
//          usage(true);
//        }
//        body = args[pos];
//      } else if ("--body-file".equals(arg)) {
//        if (++pos >= args.length) {
//          usage(true);
//        }
//        bodyFile = args[pos];
//      } else if ("--hex".equals(arg)) {
//        hex = true;
//      } else if ("--url".equals(arg)) {
//        urlenc = true;
//      } else if ("--all".equals(arg)) {
//        all = true;
//      } else if ("--first".equals(arg)) {
//        if (++pos >= args.length) {
//          usage(true);
//        }
//        try {
//          first = Integer.valueOf(args[pos]);
//        } catch (NumberFormatException e) {
//          usage(true);
//        }
//      } else if ("--last".equals(arg)) {
//        if (++pos >= args.length) {
//          usage(true);
//        }
//        try {
//          last = Integer.valueOf(args[pos]);
//        } catch (NumberFormatException e) {
//          usage(true);
//        }
//      } else if ("--group".equals(arg)) {
//        if (++pos >= args.length) {
//          usage(true);
//        }
//        try {
//          consumer = args[pos];
//          // validate that it is a number
//          Long.valueOf(consumer);
//        } catch (NumberFormatException e) {
//          usage(true);
//        }
//      } else if ("--ttl".equals(arg)) {
//        if (++pos >= args.length) {
//          usage(true);
//        }
//        try {
//          ttl = Long.valueOf(args[pos]);
//        } catch (NumberFormatException e) {
//          usage(true);
//        }
//      } else if ("--help".equals(arg)) {
//        usage(false);
//        return;
//      } else if ("--verbose".equals(arg)) {
//        verbose = true;
//      } else {  // unkown argument
//        usage(true);
//      }
//    }
//  }

  static List<String> supportedCommands =
    Arrays.asList("create", "send", "group", "fetch", "view", "info", "truncate", "config");

  void validateArguments(String[] args) {
    // first parse command arguments
    parseArguments(args);
    if (help) {
      return;
    }
    // first validate the command
    if (!supportedCommands.contains(command)) {
      usage("Please provide a valid command");
    }
    // verify that either --body or --body-file is given
    if ("send".equals(command) && body != null && bodyFile != null) {
      usage("Either --body or --body-file must be specified.");
    }
    // verify that a destination was given
    if (destination == null) {
      usage("A destination stream must be specified.");
    }
    if (port > 0 && hostname == null) {
      usage("A hostname must be provided when a port is specified.");
    }
    // verify that only one encoding is given for the body
    if (hex && urlenc) {
      usage("Only one of --hex or --url may be specified");
    }
    if (bodyFile != null && (hex || urlenc)) {
      usage("Options --hex and --url are incompatible with --body-file (binary input)");
    }
    // make sure that fetch command has a consumer id
    if ("fetch".equals(command) && consumer == null) {
      usage("--group must be specified for fetch");
    }
    // make sure that view command does not have contradicting options
    if ("view".equals(command)) {
      if ((all && first != null) || (all && last != null) ||
        (last != null && first != null)) {
        usage("Only one of --all, --first or --last may be specified");
      }
      if (first != null && first < 1) {
        usage("--first must be at least 1");
      }
      if (last != null && last < 1) {
        usage("--last must be at least 1");
      }
    }
    if ("create".equals(command)) {
      if (!isId(destination)) {
        usage("id is not a printable ascii character string");
      }
    }
    if ("config".equals(command)) {
      if (ttl == null || ttl < 0L) {
        usage("--ttl must be specified as a non-negative value");
      }
    }
  }

  /**
   * read the body of the event in binary form, either from
   * --body or --body-file.
   */
  byte[] readBody() {
    if (body != null) {
      if (urlenc) {
        return Util.urlDecode(body);
      } else if (hex) {
        return Util.hexValue(body);
      }
      return body.getBytes();
    } else if (bodyFile != null) {
      return Util.readBinaryFile(bodyFile);
    }
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
        if (verbose) {
          System.out.println(binaryBody.length +
                               " bytes written to file " + bodyFile + ".");
        }
        return binaryBody.length + " bytes written to file";
      } catch (IOException e) {
        System.err.println(
          "Error writing to file " + bodyFile + ": " + e.getMessage());
        return null;
      }
    }
    String body;
    // was hex encoding requested?
    if (hex) {
      body = Util.toHex(binaryBody);
      // or was URl encoding specified?
    } else if (urlenc) {
      body = Util.urlEncode(binaryBody);
      // by default, assume the same encoding for the value as for the key
    } else {
      body = new String(binaryBody);
    }

    if (verbose) {
      System.out.println(
        "Body[" + binaryBody.length + " bytes]: " + body);
    } else {
      System.out.println(body);
    }
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
   *         in case of success
   */
  public String execute0(String[] args, CConfiguration config) {
    // parse and validate arguments
    validateArguments(args);
    if (help) {
      return "";
    }
    // read the access token from file if it exists
    if (accessToken == null && tokenFile != null) {
      accessToken = readTokenFile();
    }

    // determine the base url for the HTTP requests
    String baseUrl = GatewayUrlGenerator.getBaseUrl(config, hostname, port, !forceNoSSL && apikey != null);
    if (baseUrl == null) {
      System.err.println("Can't figure out the URL to send to. Please use --host to specify");
      return null;
    } else {
      if (verbose) {
        System.out.println("Using base URL: " + baseUrl);
      }
    }

    // build the full URI for the request and validate it
    String requestUrl = baseUrl + "streams/" + destination;

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
      return (sendHttpRequest(post, null) != null) ? "OK." : null;
    } else if ("group".equals(command)) {
      String id = getConsumerId(requestUrl);
      if (id != null) {
        System.out.println(id);
        return id;
      } else {
        return null;
      }
    } else if ("info".equals(command)) {
      String info = getQueueInfo(requestUrl);
      if (info != null) {
        System.out.println(info);
        return "OK.";
      } else {
        return null;
      }
    } else if ("fetch".equals(command)) {
      StreamEvent event;
      try {
        event = fetchOne(requestUrl, consumer);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        return null;
      }
      if (event == null) {
        System.out.println("no event");
        return "";
      }
      // print all the headers
      for (String name : event.getHeaders().keySet()) {
        // unless --verbose was given, we suppress continuuity headers
        if (!verbose && name.startsWith(Constants.Gateway.CONTINUUITY_PREFIX)) {
          continue;
        }
        System.out.println(name + ": " + event.getHeaders().get(name));
      }
      // and finally write out the body
      return writeBody(Bytes.toBytes(event.getBody()));
    } else if ("view".equals(command)) {
      if (consumer == null) {
        HttpPost post = new HttpPost(requestUrl + "/consumer-id");
        HttpResponse response = sendHttpRequest(post, null);
        if (response == null) {
          return null;
        }
        // read the binary value from the HTTP response
        byte[] binaryValue = Util.readHttpResponse(response);
        if (binaryValue == null) {
          System.err.println("Unexpected response without body.");
          return null;
        }
        consumer = Bytes.toString(binaryValue);
      }
      Collector<StreamEvent> collector =
        all ? new AllCollector<StreamEvent>(StreamEvent.class) :
          first != null ? new FirstNCollector<StreamEvent>(first, StreamEvent.class) :
            last != null ? new LastNCollector<StreamEvent>(last, StreamEvent.class) :
              new FirstNCollector<StreamEvent>(10, StreamEvent.class);
      try {
        StreamEvent[] events =
          fetchAll(requestUrl, consumer, collector);
        return printEvents(events);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        return null;
      }

    } else if ("create".equals(command)) {
      HttpPut put = new HttpPut(requestUrl);
      return (sendHttpRequest(put, null) != null) ? "OK." : null;
    } else if ("truncate".equals(command)) {
      HttpPost post = new HttpPost(requestUrl + "/truncate");
      return (sendHttpRequest(post, null) != null) ? "OK." : null;
    } else if ("config".equals(command)) {
      try {
        URL url = new URL(requestUrl + "/config");
        try {
          HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
          try {
            urlConn.setDoOutput(true);
            urlConn.setRequestMethod("PUT");
            if (apikey != null) {
              urlConn.setRequestProperty(Constants.Gateway.CONTINUUITY_API_KEY, apikey);
            }
            if (accessToken != null) {
              urlConn.setRequestProperty("Authorization", "Bearer " + accessToken);
            }
            JsonObject streamConfig = new JsonObject();
            streamConfig.addProperty("ttl", ttl);
            urlConn.getOutputStream().write(GSON.toJson(streamConfig).getBytes(Charsets.UTF_8));
            if (!checkHttpStatus(urlConn.getResponseCode(), urlConn.getHeaderField(0),
                                 urlConn.getErrorStream(), Collections.singletonList(HttpStatus.SC_OK))) {
              return null;
            }
            return "OK.";
          } finally {
            urlConn.disconnect();
          }
        } catch (IOException e) {
          System.err.println("Error sending HTTP request: " + e.getMessage());
          return null;
        }
      } catch (MalformedURLException e) {
        // Shouldn't happen
        System.err.println("Error sending HTTP request to " + requestUrl + "/config");
        return null;
      }
    }
    return null;
  }

  /**
   * This implements --group, it obtains a new consumer group id.
   *
   * @param requestUrl the base url with the stream added to it
   * @return the consumer group id returned by the gateway
   */
  String getConsumerId(String requestUrl) {
    HttpPost post = new HttpPost(requestUrl + "/consumer-id");
    HttpResponse response = sendHttpRequest(post, null);
    if (response == null) {
      return null;
    }
    // read the binary value from the HTTP response
    byte[] binaryValue = Util.readHttpResponse(response);
    if (binaryValue == null) {
      System.err.println("Unexpected response without body.");
      return null;
    }
    return Bytes.toString(binaryValue);
  }

  /**
   * This implements --info, it retrieves queue meta information.
   *
   * @param requestUrl the base url with the stream added to it
   * @return the consumer group id returned by the gateway,
   *         null in case of error
   */
  String getQueueInfo(String requestUrl) {
    // prepare for HTTP
    HttpGet get = new HttpGet(requestUrl + "/info");
    HttpResponse response = sendHttpRequest(get, null);
    if (response == null) {
      return null;
    }
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
   *
   * @param uri       The request URI including the stream name without the query
   * @param consumer  the consumer group id, as previously returned by
   *                  getConsumerId()
   * @param collector a collector for the events in the stream
   * @return all events collected
   * @throws Exception if something goes wrong
   */
  StreamEvent[] fetchAll(String uri, String consumer, Collector<StreamEvent> collector)
    throws Exception {
    while (true) {
      StreamEvent event = fetchOne(uri, consumer);
      if (event == null) {
        return collector.finish();
      }
      boolean collectMore = collector.addElement(event);
      if (!collectMore) {
        return collector.finish();
      }
    }
  }

  /**
   * Helper method for --view, given a request URL already constructed and a
   * consumer ID, it fetches (dequeues) one event from the stream.
   *
   * @param uri      The request URI including the stream name without the query
   * @param consumer the consumer group id, as previously returned by
   *                 getConsumerId()
   * @return the event that was fetched, or null if the stream is empty
   * @throws Exception if something goes wrong
   */
  StreamEvent fetchOne(String uri, String consumer) throws Exception {
    // prepare for HTTP
    HttpPost post = new HttpPost(uri + "/dequeue");
    post.addHeader(Constants.Stream.Headers.CONSUMER_ID, consumer);
    Integer[] expectedCodes = { HttpStatus.SC_OK, HttpStatus.SC_NO_CONTENT };
    HttpResponse response = sendHttpRequest(post, Arrays.asList(expectedCodes));
    if (response == null) {
      return null;
    }
    // we expect either OK for an event, or NO_CONTENT for end of stream
    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NO_CONTENT) {
      return null;
    }
    // read the binary value from the HTTP response
    byte[] binaryValue = Util.readHttpResponse(response);
    if (binaryValue == null) {
      throw new Exception("Unexpected response without body.");
    }
    // collect all the headers
    Map<String, String> headers = new TreeMap<String, String>();
    for (org.apache.http.Header header : response.getAllHeaders()) {
      String name = header.getName();
      // ignore common HTTP headers. All the headers of the actual
      // event are transmitted with the stream name as a prefix
      if (name.startsWith(destination)) {
        String actualName = name.substring(destination.length() + 1);
        headers.put(actualName, header.getValue());
      }
    }
    return new DefaultStreamEvent(headers, ByteBuffer.wrap(binaryValue));
  }

  /**
   * Helper for --view. Prints all the collected events to stdout. This can be
   * improved to use better formatting, shortening, etc.
   *
   * @param events An array of events
   * @return a String indicating how many events were printed
   */
  String printEvents(StreamEvent[] events) {
    System.out.println(events.length + " events: ");
    for (StreamEvent event : events) {
      String sep = "";
      for (String name : event.getHeaders().keySet()) {
        // unless --verbose was given, we suppress continuuity headers
        if (!verbose && name.startsWith(Constants.Gateway.CONTINUUITY_PREFIX)) {
          continue;
        }
        System.out.print(sep + name + "=" + event.getHeaders().get(name));
        sep = ", ";
      }
      System.out.print(": ");
      if (hex) {
        System.out.print(Util.toHex(Bytes.toBytes(event.getBody())));
      } else if (urlenc) {
        System.out.print(Util.urlEncode(Bytes.toBytes(event.getBody())));
      } else {
        System.out.print(new String(Bytes.toBytes(event.getBody())));
      }
      System.out.println();
    }
    return events.length + " events.";
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
    String name = "stream-client";
    if (System.getProperty("script") != null) {
      name = System.getProperty("script").replaceAll("[./]", "");
    }
    StreamClient instance = new StreamClient(name);
    String value = instance.execute(args, config);
    // exit with error in case fails
    if (value == null) {
      System.exit(1);
    }
  }

  private boolean isId(String id) {
    StreamSpecification spec = new StreamSpecification.Builder().setName(id).create();
    StreamVerification verifier = new StreamVerification();
    VerifyResult result = verifier.verify(null, spec); // safe to pass in null for this verifier
    return (result.getStatus() == VerifyResult.Status.SUCCESS);
  }

}

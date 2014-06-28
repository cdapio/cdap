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
import com.continuuity.gateway.util.Util;
import com.continuuity.internal.app.verification.StreamVerification;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
  private static final String HEADER_OPTION = "header";

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

  public StreamClient() {
    super("stream-client");
  }

  public StreamClient(String toolName) {
    super(toolName);
  }

  @Override
  protected void addOptions(Options options) {
    options.addOption(OptionBuilder
                        .withLongOpt(HEADER_OPTION)
                        .hasArgs(2)
                        .withDescription("To specify a header for the event to send. " +
                                           "Can be specified multiple times.")
                        .create());
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
  public void printUsageTop(boolean error) {
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
  }

  @Override
  public boolean parseAdditionalArguments(CommandLine line) {
    destination = line.getOptionValue(STREAM_OPTION, null);
    body = line.getOptionValue(BODY_OPTION, null);
    bodyFile = line.getOptionValue(BODY_FILE_OPTION, null);
    hex = line.hasOption(HEX_OPTION);
    urlenc = line.hasOption(URL_OPTION);
    all = line.hasOption(ALL_OPTION);
    if (line.hasOption(HEADER_OPTION)) {
      String[] headerList = line.getOptionValues(HEADER_OPTION);
      // must pass header arguments in pairs
      if (headerList.length % 2 != 0) {
        usage("--" + HEADER_OPTION + " arguments must be passed in pairs");
      }
      for (int i = 0; i < headerList.length; i += 2) {
        headers.put(headerList[i], headerList[i + 1]);
      }
    }
    // validate consumer is a numerical value
    consumer = line.hasOption(GROUP_OPTION) ? parseNumericArg(line, GROUP_OPTION).toString() : null;
    first = line.hasOption(FIRST_OPTION) ? parseNumericArg(line, FIRST_OPTION).intValue() : null;
    last = line.hasOption(LAST_OPTION) ? parseNumericArg(line, LAST_OPTION).intValue() : null;
    ttl = line.hasOption(TTL_OPTION) ? parseNumericArg(line, TTL_OPTION) : null;
    // should have 1 arg remaining, the command which is positional
    String[] remaining = line.getArgs();
    if (remaining.length != 1) {
      return false;
    }
    command = remaining[0];
    return true;
  }

  static List<String> supportedCommands =
    Arrays.asList("create", "send", "group", "fetch", "view", "info", "truncate", "config");

  @Override
  protected String validateArguments() {
    // first validate the command
    if (!supportedCommands.contains(command)) {
      return ("Please provide a valid command");
    }
    // verify that either --body or --body-file is given
    if ("send".equals(command) && body != null && bodyFile != null) {
      return "Either --body or --body-file must be specified.";
    }
    // verify that a destination was given
    if (destination == null) {
      return "A destination stream must be specified.";
    }
    if (port > 0 && hostname == null) {
      return "A hostname must be provided when a port is specified.";
    }
    // verify that only one encoding is given for the body
    if (hex && urlenc) {
      return "Only one of --hex or --url may be specified";
    }
    if (bodyFile != null && (hex || urlenc)) {
      return "Options --hex and --url are incompatible with --body-file (binary input)";
    }
    // make sure that fetch command has a consumer id
    if ("fetch".equals(command) && consumer == null) {
      return "--group must be specified for fetch";
    }
    // make sure that view command does not have contradicting options
    if ("view".equals(command)) {
      if ((all && first != null) || (all && last != null) ||
        (last != null && first != null)) {
        return "Only one of --all, --first or --last may be specified";
      }
      if (first != null && first < 1) {
        return "--first must be at least 1";
      }
      if (last != null && last < 1) {
        return "--last must be at least 1";
      }
    }
    if ("create".equals(command)) {
      if (!isId(destination)) {
        return "id is not a printable ascii character string";
      }
    }
    if ("config".equals(command)) {
      if (ttl == null || ttl < 0L) {
        return "--ttl must be specified as a non-negative value";
      }
    }
    return null;
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
   * @param config The configuration of the gateway
   * @return null in case of error, an string representing the retrieved value
   *         in case of success
   */
  protected String execute(CConfiguration config) {
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
      HttpClient client = new DefaultHttpClient();
      try {
        return (sendHttpRequest(client, post, null) != null) ? "OK." : null;
      } finally {
        client.getConnectionManager().shutdown();
      }
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
        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(requestUrl + "/consumer-id");
        try {
          HttpResponse response = sendHttpRequest(client, post, null);
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
        } finally {
          client.getConnectionManager().shutdown();
        }
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
      HttpClient client = new DefaultHttpClient();
      HttpPut put = new HttpPut(requestUrl);
      try {
        return (sendHttpRequest(client, put, null) != null) ? "OK." : null;
      } finally {
        client.getConnectionManager().shutdown();
      }
    } else if ("truncate".equals(command)) {
      HttpClient client = new DefaultHttpClient();
      HttpPost post = new HttpPost(requestUrl + "/truncate");
      try {
        return (sendHttpRequest(client, post, null) != null) ? "OK." : null;
      } finally {
        client.getConnectionManager().shutdown();
      }
    } else if ("config".equals(command)) {
      HttpClient client = new DefaultHttpClient();
      HttpPut put = new HttpPut(requestUrl + "/config");
      JsonObject streamConfig = new JsonObject();
      streamConfig.addProperty("ttl", ttl);
      put.setEntity(new ByteArrayEntity(GSON.toJson(streamConfig).getBytes(Charsets.UTF_8)));
      try {
        return (sendHttpRequest(client, put, null) != null) ? "OK." : null;
      } finally {
        client.getConnectionManager().shutdown();
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
    HttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(requestUrl + "/consumer-id");
    try {
      HttpResponse response = sendHttpRequest(client, post, null);
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
    } finally {
      client.getConnectionManager().shutdown();
    }
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
    HttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(requestUrl + "/info");

    try {
      HttpResponse response = sendHttpRequest(client, get, null);
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
    } finally {
      client.getConnectionManager().shutdown();
    }
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
    HttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(uri + "/dequeue");
    post.addHeader(Constants.Stream.Headers.CONSUMER_ID, consumer);
    Integer[] expectedCodes = { HttpStatus.SC_OK, HttpStatus.SC_NO_CONTENT };

    try {
      HttpResponse response = sendHttpRequest(client, post, Arrays.asList(expectedCodes));
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
    } finally {
      client.getConnectionManager().shutdown();
    }
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

  private boolean isId(String id) {
    StreamSpecification spec = new StreamSpecification.Builder().setName(id).create();
    StreamVerification verifier = new StreamVerification();
    VerifyResult result = verifier.verify(null, spec); // safe to pass in null for this verifier
    return (result.getStatus() == VerifyResult.Status.SUCCESS);
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
}

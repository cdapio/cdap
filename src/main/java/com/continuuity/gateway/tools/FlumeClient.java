package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.collector.FlumeCollector;
import com.continuuity.gateway.util.Util;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

// todo document this
public class FlumeClient {

  private static final Logger LOG = LoggerFactory
      .getLogger(FlumeClient.class);

  /**
   * Retrieves the port number of the flume collector from the gateway
   * configuration. If no name is passed in, tries to figures out the name
   * by scanning through the configuration.
   *
   * @param config    The gateway configuration
   * @param flumeName The name of the flume collector, optional
   * @return The port number if found, or -1 otherwise.
   */
  static int findFlumePort(CConfiguration config, String flumeName) {

    if (flumeName == null) {
      // find the name of the flume collector
      flumeName = Util.findConnector(config, FlumeCollector.class);
      if (flumeName == null) {
        return -1;
      } else {
        LOG.info("Reading configuration for connector '" + flumeName + "'.");
      }
    }
    // get the collector's port number from the config
    return config.getInt(Constants.buildConnectorPropertyName(
        flumeName, Constants.CONFIG_PORT), FlumeCollector.DefaultPort);
  }

  public static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    out.println("Usage: FlumeClient <option> ... with");
    out.println("  --port <number>         To specify the port to use");
    out.println("  --host <name>           To specify the hostname to send to");
    out.println("  --connector <name>      To specify the name of the flume connector");
    out.println("  --stream <name>         To specify the destination event stream");
    out.println("	 --header <name> <value> To specify a header for the event to send. Can be used multiple times");
    out.println("  --body <value>          To specify the body of the event");
    out.println("  --file <path>           To specify a file containing the body of the event");
    if (error) System.exit(1);
  }

  public static void main(String[] args) {

    int port = -1;
    String hostname = "localhost";
    String connector = null;
    String filename = null;
    String destination = null;
    SimpleEvent event = new SimpleEvent();

    for (int pos = 0; pos < args.length; pos++) {
      String arg = args[pos];
      if ("--port".equals(arg)) {
        if (++pos >= args.length) usage(true);
        try {
          port = Integer.valueOf(args[pos]);
          continue;
        } catch (NumberFormatException e) {
          usage(true);
        }
      } else if ("--host".equals(arg)) {
        if (++pos >= args.length) usage(true);
        hostname = args[pos];
        continue;
      } else if ("--connector".equals(arg)) {
        if (++pos >= args.length) usage(true);
        connector = args[pos];
        continue;
      } else if ("--stream".equals(arg)) {
        if (++pos >= args.length) usage(true);
        destination = args[pos];
        continue;
      } else if ("--header".equals(arg)) {
        if (pos + 2 >= args.length) usage(true);
        event.getHeaders().put(args[++pos], args[++pos]);
        continue;
      } else if ("--body".equals(arg)) {
        if (++pos >= args.length) usage(true);
        event.setBody(args[pos].getBytes());
        continue;
      } else if ("--file".equals(arg)) {
        if (++pos >= args.length) usage(true);
        filename = args[pos];
        continue;
      } else if ("--help".equals(arg)) {
        usage(false);
        System.exit(0);
      }
      // unkown argument
      usage(true);
    }

    if (port < 0) {
      CConfiguration config = CConfiguration.create();
      port = findFlumePort(config, connector);
    }
    if (port < 0) {
      System.err.println("Can't figure out the port to send to. Please use --port or --connector to specify.");
    } else {
      System.out.println("Using port: " + port);
    }

    if (destination != null) {
      event.getHeaders().put(Constants.HEADER_DESTINATION_STREAM, destination);
    }

    if (filename != null) {
      byte[] bytes = Util.readBinaryFile(filename);
      if (bytes == null) {
        System.err.println("Cannot read body from file " + filename + ".");
        System.exit(1);
      }
      event.setBody(bytes);
    }

    if (event.getBody() == null) {
      System.err.println("Cannot send an event without body. Please use --body or --file to specify the body.");
      System.exit(1);
    }

    // event is now fully constructed, ready to send
    RpcClient client = RpcClientFactory.getDefaultInstance(hostname, port, 1);
    try {
      client.append(event);
    } catch (EventDeliveryException e) {
      client.close();
      System.err.println("Error sending flume event: " + e.getMessage());
      System.exit(1);
    }
    client.close();
  }
}

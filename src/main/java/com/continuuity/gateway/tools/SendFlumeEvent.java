package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.collector.FlumeCollector;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class SendFlumeEvent {

	private static final Logger LOG = LoggerFactory
			.getLogger(SendFlumeEvent.class);

	/**
	 * Reads the gateway configuration, figures out the name of the flume
	 * collector - if one is configured - and returns its port number.
	 * @param config The gateway configuration
	 * @return The port number if found, or -1 otherwise.
	 */
	// todo move this to util and write a unit test
	static int findFlumePort(CConfiguration config, String flumeName) {

		if (flumeName == null) {
			List<String> flumeConnectors = new LinkedList<String>();

			// Retrieve the list of connectors in the gateway
			Collection<String> connectorNames = config.
					getStringCollection(Constants.CONFIG_CONNECTORS);

			// For each Connector
			for (String connectorName : connectorNames) {
				// Retrieve the connector's Class name
				String connectorClassName = config.get(
						Constants.buildConnectorPropertyName(connectorName,
								Constants.CONFIG_CLASSNAME));
				// no class name configured? skip!
				if (connectorClassName == null) {
					LOG.warn("No class configured for connector '" + connectorName + "'.");
					continue;
				}
		 		try {
					// test whether this connector is a subclass of FlumeCollector -> hit!
					Class connectorClass = Class.forName(connectorClassName);
					if (FlumeCollector.class.isAssignableFrom(connectorClass)) {
						flumeConnectors.add(connectorName);
					}
					// class cannot be found? skip!
				} catch (ClassNotFoundException e) {
					 LOG.warn("Configured class " + connectorClassName +
							 " for connector '" + connectorName + "' not found.");
				}
			}
			// make sure there is exactly one flume collector
			if (flumeConnectors.size() == 0) {
				LOG.error("No flume collector found in configuration.");
				return -1;
			} else if (flumeConnectors.size() > 1) {
				LOG.error("Multiple flume collectors found: " + flumeConnectors);
				return -1;
			}
			LOG.info("Reading configuration for connector '" + flumeName + "'.");
			flumeName = flumeConnectors.iterator().next();
		}
		// get that single collector's port number from the config
		int flumePort = config.getInt(Constants.buildConnectorPropertyName(
				flumeName, Constants.CONFIG_PORT), FlumeCollector.DefaultPort);

		return flumePort;
	}

	public static void usage(boolean error) {
		PrintStream out = (error ? System.err : System.out);
		out.println("Usage: SendFlumeEvent <option> ... with");
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
			}
			else if ("--host".equals(arg)) {
				if (++pos >= args.length) usage(true);
				hostname = args[pos];
				continue;
			}
			else if ("--connector".equals(arg)) {
				if (++pos >= args.length) usage(true);
				connector = args[pos];
				continue;
			}
			else if ("--stream".equals(arg)) {
				if (++pos >= args.length) usage(true);
				destination = args[pos];
				continue;
			}
			else if ("--header".equals(arg)) {
				if (pos + 2 >= args.length) usage(true);
				event.getHeaders().put(args[++pos], args[++pos]);
				continue;
			}
			else if ("--body".equals(arg)) {
				if (++pos >= args.length) usage(true);
				event.setBody(args[pos].getBytes());
				continue;
			}
			else if ("--file".equals(arg)) {
				if (++pos >= args.length) usage(true);
				filename = args[pos];
				continue;
			}
			else if ("--help".equals(arg)) {
				usage(false);
				System.exit(0);
			}
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
			File file = new File(filename);
			if (!file.isFile()) {
				System.err.println("'" + filename + "' is not a regular file.");
			}
			int bytesToRead = (int)file.length();
			byte[] bytes = new byte[bytesToRead];
			int offset = 0;
			try {
				FileInputStream input = new FileInputStream(filename);
				while (bytesToRead > 0) {
					int bytesRead = input.read(bytes, offset, bytesToRead);
					bytesToRead -= bytesRead;
					offset += bytesRead;
				}
				input.close();
			} catch (FileNotFoundException e) {
				System.err.println("File '" + filename + "' cannot be opened: " + e.getMessage());
				System.exit(1);
			} catch (IOException e) {
				System.err.println("Error reading from file '" + filename + "': " + e.getMessage());
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

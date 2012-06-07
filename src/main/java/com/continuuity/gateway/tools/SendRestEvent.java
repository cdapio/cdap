package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.util.HttpConfig;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class SendRestEvent {

	private static final Logger LOG = LoggerFactory
			.getLogger(SendRestEvent.class);

	/**
	 * Reads the gateway configuration, figures out the name of the rest
	 * collector - if one is configured - and returns its HttpConfig.
	 * @param config The gateway configuration
	 * @param restName The name of the rest collector, optional
	 * @return the HttpConfig if successful, or null otherwise.
	 */
	// todo move this to util and write a unit test
	static HttpConfig findRestConfig(CConfiguration config, String restName) {

		if (restName == null) {
			List<String> restConnectors = new LinkedList<String>();

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
					if (RestCollector.class.isAssignableFrom(connectorClass)) {
						restConnectors.add(connectorName);
					}
					// class cannot be found? skip!
				} catch (ClassNotFoundException e) {
					LOG.warn("Configured class " + connectorClassName +
							" for connector '" + connectorName + "' not found.");
				}
			}
			// make sure there is exactly one flume collector
			if (restConnectors.size() == 0) {
				LOG.error("No Rest collector found in configuration.");
				return null;
			} else if (restConnectors.size() > 1) {
				LOG.error("Multiple Rest collectors found: " + restConnectors);
				return null;
			}
			restName = restConnectors.iterator().next();
			LOG.info("Reading configuration for connector '" + restName + "'.");
		}
		// get that collector's http config
		HttpConfig httpConfig = null;
		try {
			httpConfig = HttpConfig.configure(restName, config, null);
		} catch (Exception e) {
			LOG.error("Exception reading Http configuration for connector '"
					+ restName + "': " + e.getMessage());
		}
		return httpConfig;
	}

	public static void usage(boolean error) {
		PrintStream out = (error ? System.err : System.out);
		out.println("Usage: SendFlumeEvent <option> ... with");
		out.println("  --base <url>            To specify the port to use");
		out.println("  --host <name>           To specify the hostname to send to");
		out.println("  --connector <name>      To specify the name of the flume connector");
		out.println("  --stream <name>         To specify the destination event stream");
		out.println("	 --header <name> <value> To specify a header for the event to send. Can be used multiple times");
		out.println("  --body <value>          To specify the body of the event");
		out.println("  --file <path>           To specify a file containing the body of the event");
		if (error) System.exit(1);
	}

	public static void main(String[] args) {

		String baseUrl = null;
		String hostname = "localhost";
		String connector = null;
		String filename = null;
		String destination = null;
		HttpPost post = new HttpPost();

		for (int pos = 0; pos < args.length; pos++) {
			String arg = args[pos];
			if ("--base".equals(arg)) {
				if (++pos >= args.length) usage(true);
				baseUrl = args[pos];
				continue;
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
				post.setHeader(args[++pos], args[++pos]);
				continue;
			}
			else if ("--body".equals(arg)) {
				if (++pos >= args.length) usage(true);
				post.setEntity(new ByteArrayEntity(args[pos].getBytes()));
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
			// unkown argument
			usage(true);
		}

		if (baseUrl == null) {
			CConfiguration config = CConfiguration.create();
			HttpConfig httpConfig = findRestConfig(config, connector);
			if (httpConfig != null) {
				baseUrl = httpConfig.getBaseUrl(hostname);
			}
		}
		if (baseUrl == null) {
			System.err.println("Can't figure out the URL to send to. Please use --base or --connector to specify.");
		} else {
			System.out.println("Using base URL: " + baseUrl);
		}

		// add the stream name to the baseUrl
		if (destination == null) {
			System.err.println("Need a stream name to form REST url.");
			System.exit(1);
		}

		String url = baseUrl + destination;
		try {
			URI uri = new URI(url);
			post.setURI(uri);
		} catch (URISyntaxException e) {
			System.err.println("URI " + url + " is not well-formed. Giving up.");
			System.exit(1);
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
			post.setEntity(new ByteArrayEntity(bytes));
		}

		if (post.getEntity() == null) {
			System.err.println("Cannot send an event without body. Please use --body or --file to specify the body.");
			System.exit(1);
		}

		// post is now fully constructed, ready to send
		HttpClient client = new DefaultHttpClient();
		try {
			HttpResponse response = client.execute(post);
			int status = response.getStatusLine().getStatusCode();
			if (status != HttpStatus.SC_OK) {
				System.err.println("Error sending event: " + response.getStatusLine());
				System.exit(1);
			}
		} catch (IOException e) {
			System.err.println("Exception when sending event: " + e.getMessage());
		} finally {
			client.getConnectionManager().shutdown();
		}
	}
}

package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.accessor.RestAccessor;
import com.continuuity.gateway.util.HttpConfig;
import com.continuuity.gateway.util.Util;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * This is a command line tool to retrieve a value by key from the
 * data fabric.
 * <ul>
 *   <li>It attempts to be smart and determine the URL of the REST
 *   accessor auto-magically. If that fails, the user can give hints
 *   via the --connector and --base arguments</li>
 *   <li>The key can be read in binary form from a file, or provided
 *   on the command line as a String in various encodings, as indicated
 *   by the --hex, --url, and --encoding arguments</li>
 *   <li>The value can be saved to a file in binary form, or printed
 *   to the screen in the same encoding as the key.</li>
 * </ul>
 */
public class GetValueByKey {

	private static final Logger LOG = LoggerFactory
			.getLogger(GetValueByKey.class);

	/**
	 * Retrieves the http config of the rest accessor from the gateway
	 * configuration. If no name is passed in, tries to figures out the
	 * name by scanning through the configuration. Then it uses the
	 * obtained Http config to create the base url for requests.
	 * @param config The gateway configuration
	 * @param restName The name of the rest accessor, optional
	 * @param hostname The hostname to use for the url, optional
	 * @return The base url if found, or null otherwise.
	 */
	public static String findBaseUrl(CConfiguration config, String restName, String hostname) {

		if (restName == null) {
			// find the name of the REST accessor
			restName = Util.findConnector(config, RestAccessor.class);
			if (restName == null) {
				return null;
			} else {
				LOG.info("Reading configuration for connector '" + restName + "'.");
			}
		}
		// get the collector's http config
		HttpConfig httpConfig = null;
		try {
			httpConfig = HttpConfig.configure(restName, config, null);
		} catch (Exception e) {
			LOG.error("Exception reading Http configuration for connector '"
					+ restName + "': " + e.getMessage());
			return null;
		}
		return httpConfig.getBaseUrl(hostname);
	}

	/**
	 * Print the usage statement and return null (or empty string if this is not an error case).
	 * See getValue() for an explanation of the return type.
	 * @param error indicates whether this was invoked as the result of an error
	 * @return null in case of error, an empty string in case of success
	 */
	public static String usage(boolean error) {
		PrintStream out = (error ? System.err : System.out);
		out.println("Usage: SendRestEvent <option> ... with");
		out.println("  --base <url>            To specify the port to use");
		out.println("  --host <name>           To specify the hostname to send to");
		out.println("  --connector <name>      To specify the name of the rest collector");
		out.println("  --key <string>          To specify the key");
		out.println("  --key-file <path>       To read the binary key from a file");
		out.println("  --to-file <path>        To write the binary value to a file");
		out.println("  --hex                   Value for --key is hexadecimal (value will be printed the same way)");
		out.println("  --ascii                 Value for --key is ASCII (value will be printed the same way)");
		out.println("  --url                   Value for --key is URL-encoded (value will be printed the same way)");
		out.println("  --encoding <name>       Value for --key is in this encoding (value will be printed the same way)");
		return error ? null : "";
	}

	/**
	 * This is actually the main method, but in order to make it testable, instead of exiting in case
	 * of error it returns null, whereas in case of success it returns the retrieved value as shown
	 * on the console.
 	 * @param args the command line arguments of the main method
	 * @param config The configuration of the gateway
	 * @return null in case of error, an string representing the retrieved value in case of success
	 */
	public static String getValue(String[] args, CConfiguration config) {

		// options configured by command line args
		String baseUrl = null; 	       // the base url for GET
		String hostname = "localhost"; // the hostname of the gateway
		String connector = null;       // the name of the rest accessor
		String key = null;             // the key to look up
		String keyfile = null;         // the file to read the key from
		String tofile = null;          // the file to write the value to
		String encoding = "ASCII";     // the encoding for --key and for display of the value
		boolean hexEncoded = false;    // whether --key and display of value use hexadecimal encoding
		boolean urlEncoded = false;    // whether --key and display of value use url encoding

		// go through all the arguments
		for (int pos = 0; pos < args.length; pos++) {
			String arg = args[pos];
			if ("--base".equals(arg)) {
				if (++pos >= args.length) return usage(true);
				baseUrl = args[pos];
			}
			else if ("--host".equals(arg)) {
				if (++pos >= args.length) return usage(true);
				hostname = args[pos];
			}
			else if ("--connector".equals(arg)) {
				if (++pos >= args.length) return usage(true);
				connector = args[pos];
			}
			else if ("--key".equals(arg)) {
				if (++pos >= args.length) return usage(true);
				key = args[pos];
			}
			else if ("--keyfile".equals(arg)) {
				if (++pos >= args.length) return usage(true);
				keyfile = args[pos];
			}
			else if ("--tofile".equals(arg)) {
				if (++pos >= args.length) return usage(true);
				tofile = args[pos];
			}
			else if ("--encoding".equals(arg)) {
				if (++pos >= args.length) return usage(true);
				encoding = args[pos];
			}
			else if ("--ascii".equals(arg)) {
				encoding = "ASCII";
			}
			else if ("--url".equals(arg)) {
				urlEncoded = true;
			}
			else if ("--hex".equals(arg)) {
				hexEncoded = true;
			}
			else if ("--help".equals(arg)) {
				return usage(false);
			} else {	// unkown argument
				return usage(true);
			}
		}
		// ensure that a key was given
		if (key == null && keyfile == null) {
			System.err.println("No key specified. ");
			return usage(true);
		}

		// determine the base url for the GET request
		if (baseUrl == null) {
			baseUrl = findBaseUrl(config, connector, hostname);
		}
		if (baseUrl == null) {
			System.err.println("Can't figure out the URL to send to. Please use --base or --connector to specify.");
			return null;
		} else {
			System.out.println("Using base URL: " + baseUrl);
		}

		// read the key
		byte[] binaryKey = null;
		String urlEncodedKey = null;

		// is the key in a file?
		if (keyfile != null) {
			byte[] bytes = Util.readBinaryFile(keyfile);
			if (bytes == null) {
				System.err.println("Cannot read body from file " + keyfile + ".");
				return null;
			}
			binaryKey = bytes;
		}
		// or is it --key in hexadecimal?
		else if (hexEncoded) {
			try {
				binaryKey = Util.hexValue(key);
			} catch (NumberFormatException e) {
				System.err.println("Cannot parse '" + key + "' as hexadecimal: " + e.getMessage());
				return null;
			}
		}
		// or --key in URL encoding?
		else if (urlEncoded) {
			urlEncodedKey = key;
			try {
				binaryKey = URLDecoder.decode(key, "ISO8859_1").getBytes("ISO8859_1");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				return null;
			}
		}
		// lastly, it must be --key with the given encoding (or the default)
		else if (encoding != null) {
			try {
				binaryKey = key.getBytes(encoding);
			} catch (UnsupportedEncodingException e) {
				System.err.println("Unsupported encoding " + encoding);
				return null;
			}
		}

		// url encode the key (if --url was given, then --key was already url encoded)
		if (urlEncodedKey == null) {
			if (binaryKey == null) {
				System.err.println("Key must be specified.");
				return null;
			}
			try {
				urlEncodedKey = URLEncoder.encode(new String(binaryKey, "ISO8859_1"), "ISO8859_1");
			} catch (UnsupportedEncodingException e) {
				// this cannot happen
				e.printStackTrace();
				return null;
			}
		}

		// construct the full URL and verify its well-formedness
		String fullUrl = baseUrl + "default/" + urlEncodedKey;
		URI uri;
		try {
			uri = URI.create(fullUrl);
		} catch (IllegalArgumentException e) {
			// this can only happen if the --key argument was not a valid URL string
			System.err.println("'" + urlEncodedKey + "' is not a valid URL encoded string.");
			return null;
		}

		// send an HTTP get
		HttpClient client = new DefaultHttpClient();
		HttpResponse response;
		try {
			 response = client.execute(new HttpGet(uri));
		} catch (IOException e) {
			System.err.println("Error sending HTTP request: " + e.getMessage());
			return null;
		}
		client.getConnectionManager().shutdown();

		// show the HTTP status and verify it was successful
		System.out.println(response.getStatusLine());
		if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
			return null;
		}

		// read the binary value from the HTTP response
		byte[] binaryValue;
		String value = null;

		try {
			int length = (int)response.getEntity().getContentLength();
			InputStream content = response.getEntity().getContent();
			binaryValue = new byte[length];
			int offset = 0;
			while (length > 0) { // must iterate because input stream is not guaranteed to return all at once
				int bytesRead = content.read(binaryValue, offset, length);
				offset += bytesRead; length -= bytesRead;
			}
		} catch (IOException e) {
			System.err.println("Cannot read value from HTTP response: " + e.getMessage());
			return null;
		}

		// now make value available to user
		// if so requested, write it to a file
		if (tofile != null) {
			try {
				FileOutputStream out = new FileOutputStream(tofile);
				out.write(binaryValue);
				out.close();
				System.out.println(binaryValue.length + " bytes written to file " + tofile + ".");
				return binaryValue.length + " bytes written to file";
			} catch (IOException e) {
				System.err.println("Error writing to file " + tofile + ": " + e.getMessage());
				return null;
			}
		}
		// convert to hex if key was given as hex
		if (hexEncoded) {
			value = Util.toHex(binaryValue);
		}
		// or was the key URL encoded? then we also URL encode the value
		else if (urlEncoded) {
			try {
				value = URLEncoder.encode(new String(binaryValue, "ISO8859_1"), "ISO8859_1");
			} catch (UnsupportedEncodingException e) {
				// this cannot happen
				e.printStackTrace();
			}
		}
		// by default, assume the same encoding for the value as for the key
		else {
			try {
				value = new String(binaryValue, encoding);
			} catch (UnsupportedEncodingException e) {
				System.err.println("Unsupported encoding " + encoding);
				return null;
			}
		}
		System.out.println("Value[" + binaryValue.length + " bytes]: " + value);
		return value;
	}

		/**
		 * This is the main method. It delegates to getValue() in order to make
		 * it possible to test the return value.
		 */
	public static void main(String[] args) {
		String value = getValue(args, CConfiguration.create());
		if (value == null) {
			System.exit(1);
		}
	}
}


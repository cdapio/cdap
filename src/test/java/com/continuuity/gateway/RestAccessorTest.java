package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.type.WriteOperation;
import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.continuuity.gateway.accessor.RestAccessor;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public class RestAccessorTest {

	private static final Logger LOG = LoggerFactory.getLogger(RestAccessorTest.class);

	/** this is the executor for all access to the data fabric */
	private OperationExecutor executor;

	/**
	 * Set up in-memory data fabric
	 */
	@Before
	public void setup()  {

		// Set up our Guice injections
		Injector injector = Guice.createInjector(
				new DataFabricInMemoryModule());
		this.executor = injector.getInstance(OperationExecutor.class);

	} // end of setupGateway

	/**
	 * Create a new rest accessor with a given name
	 * @param name The name for the accessor
	 * @return the accessor
	 */
	RestAccessor newAccessor(String name) {
		RestAccessor accessor = new RestAccessor();
		accessor.setName(name);
		return accessor;
	}

	/**
	 * Verify that a given value can be retrieved for a given key via http GET request
	 * @param baseUri The URI for get request, without the key
	 * @param key The key
	 * @param value The value
	 * @throws Exception if an exception occurs
	 */
	void testKeyValue(String baseUri, byte[] key, byte[] value) throws Exception {
		// add the key/value to the data fabric
		Write write = new Write(key, value);
		List<WriteOperation> operations = new ArrayList<WriteOperation>(1);
		operations.add(write);
		Assert.assertTrue(this.executor.execute(operations).isSuccess());

		// make a get URL
		String getUrl = baseUri + URLEncoder.encode(new String(key, "ISO8859_1"), "ISO8859_1");
		LOG.info("GET request URI for key '" + new String(key) + "' is " + getUrl);

		// and issue a GET request to the server
		HttpClient client = new DefaultHttpClient();
		HttpResponse response = client.execute(new HttpGet(getUrl));
		client.getConnectionManager().shutdown();

		// verify the response is ok
		Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());

		// verify the length of the return value is the same as the original value's
		int length = (int)response.getEntity().getContentLength();
		Assert.assertEquals(value.length, length);

		// verify that the value is actually the same
		InputStream content = response.getEntity().getContent();
		byte[] bytes = new byte[length];
		content.read(bytes);
		Assert.assertArrayEquals(value, bytes);
	}

	/**
	 * Verify that a given value can be retrieved for a given key via http GET request.
	 * This converts the key and value from String to bytes and calls the byte-based
	 * method testKeyValue.
	 * @param baseUri The URI for get request, without the key
	 * @param key The key
	 * @param value The value
	 * @throws Exception if an exception occurs
	 */
	void testKeyValue(String baseUri, String key, String value) throws Exception {
		testKeyValue(baseUri, key.getBytes(), value.getBytes());
	}

	/**
	 * Starts up a REST accessor, then tests retrieval of several combinations of keys and values
	 * <ul>
	 *   <li>of ASCII letters only</li>
	 *   <li>of special characters</li>
	 *   <li>containing non-ASCII characters</li>
	 *   <li>empty key or value</li>
	 *   <li>containing null bytes</li>
	 * </ul>
	 * @throws Exception if any exception occurs
	 */
	@Test
	public void testGetAccess() throws Exception {
		// some random configuration values
		String name = "restor";
		String prefix = "/v0.1";
		String path = "/table/";
		int port = Util.findFreePort();

		// configure accessor
		CConfiguration configuration = new CConfiguration();
		configuration.setInt(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
		configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_STREAM), path);
		Accessor accessor = newAccessor(name);
		accessor.configure(configuration);
		accessor.setExecutor(this.executor);

		// start the accessor
		accessor.start();

		// now perform various requests
		String uri = "http://localhost:" + port + prefix + path + "default/";

		testKeyValue(uri, "x", "y");
		testKeyValue(uri, "mt", "");
		testKeyValue(uri, "", "");
		testKeyValue(uri, "blank in the key", "some string");
		testKeyValue(uri, "cat's name?", "pfunk!");
		testKeyValue(uri, "special-/?@:%+key", "moseby");
		testKeyValue(uri, "nønäscîi", "value\u0000with\u0000nulls");
		testKeyValue(uri, "key\u0000with\u0000nulls", "foo");

		// shut it down
		accessor.stop();
	}

	// TODO test not found
	// TODO test POST
	// TODO test URL with parameter
	// TODO test URL without key
	// TODO test URL with wrong path
	// TODO test URL with table other than default
	// TODO test start/stop/restart/stop
}

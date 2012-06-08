package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.continuuity.gateway.accessor.RestAccessor;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), path);
		Accessor accessor = newAccessor(name);
		accessor.configure(configuration);
		accessor.setExecutor(this.executor);

		// start the accessor
		accessor.start();

		// now perform various requests
		String uri = "http://localhost:" + port + prefix + path + "default/";

		Util.testKeyValue(this.executor, uri, "x", "y");
		Util.testKeyValue(this.executor, uri, "mt", "");
		Util.testKeyValue(this.executor, uri, "blank in the key", "some string");
		Util.testKeyValue(this.executor, uri, "cat's name?", "pfunk!");
		Util.testKeyValue(this.executor, uri, "special-/?@:%+key", "moseby");
		Util.testKeyValue(this.executor, uri, "nønäscîi", "value\u0000with\u0000nulls");
		Util.testKeyValue(this.executor, uri, "key\u0000with\u0000nulls", "foo");

		// shut it down
		accessor.stop();
	}

	@Test
	public void testBadRequests() throws Exception {
		// configure an accessor
		final String name = "access.rest";
		final String prefix = "/continuuity";
		final String path = "/table/";
		final int port = Util.findFreePort();

		CConfiguration configuration = new CConfiguration();
		configuration.set(Constants.CONFIG_CONNECTORS, name);
		configuration.set(Constants.buildConnectorPropertyName(name,
				Constants.CONFIG_CLASSNAME), RestAccessor.class.getCanonicalName());
		configuration.setInt(Constants.buildConnectorPropertyName(name,
				Constants.CONFIG_PORT),port);
		configuration.set(Constants.buildConnectorPropertyName(name,
				Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.buildConnectorPropertyName(name,
				Constants.CONFIG_PATH_MIDDLE), path);

		// create, configure, and start Accessor
		RestAccessor accessor = new RestAccessor();
		accessor.setName(name);
		accessor.setExecutor(this.executor);
		accessor.configure(configuration);
		accessor.start();

		// the correct URL would be http://localhost:<port>/continuuity/table/
		String baseUrl = accessor.getHttpConfig().getBaseUrl() + "default/";

		// test one valid key
		Util.testKeyValue(this.executor, baseUrl, "x", "y");

		// submit a request without prefix in the path -> 404 Not Found
		Assert.assertEquals(404, Util.sendGetRequest("http://localhost:" + port + "/somewhere"));
		Assert.assertEquals(404, Util.sendGetRequest("http://localhost:" + port + "/continuuity/data"));

		// submit a request with correct prefix but no table -> 404 Not Found
		Assert.assertEquals(404, Util.sendGetRequest("http://localhost:" + port + "/continuuity/table/x"));

		// submit a request with correct prefix but non-existent table -> 404 Not Found
		Assert.assertEquals(404, Util.sendGetRequest("http://localhost:" + port + "/continuuity/table/other/x"));

		// submit a POST to the accessor (which only supports GET) -> 405 Not Allowed
		Assert.assertEquals(405, Util.sendPostRequest(baseUrl));

		// submit a GET without key -> 404 Not Found
		Assert.assertEquals(404, Util.sendGetRequest(baseUrl));

		// submit a GET with existing key -> 200 OK
		Assert.assertEquals(200, Util.sendGetRequest(baseUrl + "x"));

		// submit a GET with non-existing key -> 404 Not Found
		Assert.assertEquals(404, Util.sendGetRequest(baseUrl + "does.not.exist"));

		// submit a GET with existing key but more after that in the path -> 404 Not Found
		Assert.assertEquals(404, Util.sendGetRequest(baseUrl + "x/y/z"));

		// submit a GET with existing key but with query part -> 501 Not Implemented
		Assert.assertEquals(501, Util.sendGetRequest(baseUrl + "x?query=none"));
	}

	// TODO test start/stop/restart/stop
}

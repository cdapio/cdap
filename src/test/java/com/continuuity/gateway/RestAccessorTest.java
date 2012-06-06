package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.continuuity.gateway.accessor.RestAccessor;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
		configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_STREAM), path);
		Accessor accessor = newAccessor(name);
		accessor.configure(configuration);
		accessor.setExecutor(this.executor);

		// start the accessor
		accessor.start();

		// now perform various requests
		String uri = "http://localhost:" + port + prefix + path + "default/";

		Util.testKeyValue(this.executor, uri, "x", "y");
		Util.testKeyValue(this.executor, uri, "mt", "");
		Util.testKeyValue(this.executor, uri, "", "");
		Util.testKeyValue(this.executor, uri, "blank in the key", "some string");
		Util.testKeyValue(this.executor, uri, "cat's name?", "pfunk!");
		Util.testKeyValue(this.executor, uri, "special-/?@:%+key", "moseby");
		Util.testKeyValue(this.executor, uri, "nønäscîi", "value\u0000with\u0000nulls");
		Util.testKeyValue(this.executor, uri, "key\u0000with\u0000nulls", "foo");

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

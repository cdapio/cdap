package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.type.WriteOperation;
import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.continuuity.gateway.accessor.RestAccessor;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RestAccessorTest {

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

	RestAccessor newAccessor(String name) {
		RestAccessor accessor = new RestAccessor();
		accessor.setName(name);
		return accessor;
	}

	@Test
	public void testReadValue() throws Exception {
		// some random configuration values
		String name = "restor";
		String prefix = "/v0.1";
		String path = "/table/";
		int port = Util.findFreePort();
		String key = "name";
		String value = "pfunk the cat";

		// add the value to the data fabric
		Write write = new Write(key.getBytes(), value.getBytes());
		List<WriteOperation> operations = new ArrayList<WriteOperation>(1);
		operations.add(write);
		Assert.assertTrue(this.executor.execute(operations).isSuccess());

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
		String url = "http://localhost:" + port + prefix + path + "default/" + key;
		// HttpGet get = new HttpGet(url);
		System.out.println(url);

		// keep it running for a while
		Thread.sleep(100 * 1000);
	}

}

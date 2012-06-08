package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.type.WriteOperation;
import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.continuuity.gateway.accessor.RestAccessor;
import com.continuuity.gateway.tools.GetValueByKey;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class GetValueByKeyTest {

	private OperationExecutor executor = null;

	/**
	 * Set up our data fabric and insert some test key/value pairs.
	 */
	@Before
	public void setupDataFabric() throws Exception {
		// Set up our Guice injections
		Injector injector = Guice.createInjector(
				new DataFabricInMemoryModule());
		this.executor = injector.getInstance(OperationExecutor.class);

		String[][] keyValues = {
				{ "cat", "pfunk" }, // a simple key and value
				{ "the cat", "pfunk" }, // a key with a blank
				{ "k\u00eby", "v\u00e4l\u00fce" } // key and value with non-ascii characters
		};
		// create a batch of writes
		List<WriteOperation> operations = new ArrayList<WriteOperation>(keyValues.length);
		for (String[] kv : keyValues) {
			operations.add(new Write(kv[0].getBytes("ISO8859_1"), kv[1].getBytes("ISO8859_1")));
		}
		// execute the batch and ensure it was successful
		BatchOperationResult result = executor.execute(operations);
		Assert.assertTrue(result.isSuccess());
	}

	/**
	 * This tests the GetKeyByValue tool for various combinations of
	 * command line arguments. Note that this tool is a command line tool,
	 * and it prints stuff on the console. That is not testable with this
	 * unit test. Therefore we only test whether it succeeds or fails for
	 * certain command line argument.
	 * @throws Exception if anything goes wrong
	 */
	@Test
	public void testUsage() throws Exception {

		// configure a gateway
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

		// Now create our Gateway with a dummy consumer (we don't run collectors)
		// and make sure to pass the data fabric executor to the gateway.
		Gateway gateway = new Gateway();
		gateway.setExecutor(this.executor);
		gateway.setConsumer(new Util.NoopConsumer());
		gateway.configure(configuration);
		gateway.start();

		// argument combinations that should return success
		String[][] goodArgsList = {
				{ "--help" }, // print help
				{ "--key", "cat" }, // simple key
				{ "--key", "k\u00eby", "--encoding", "Latin1"}, // non-ascii key with latin1 encoding
				{ "--key", "636174", "--hex" }, // "cat" in hex notation
				{ "--key", "6beb79", "--hex" }, // non-Ascii "këy" in hex notation
				{ "--key", "cat", "--base", "http://localhost:" + port + prefix + path }, // explicit base url
				{ "--key", "cat", "--host", "localhost" }, // correct hostname
				{ "--key", "cat", "--connector", name }, // valid connector name
		};

		// argument combinations that should lead to failure
		String[][] badArgsList = {
				{ },
				{ "--key" }, // no key
				{ "--garble" }, // invalid argument
				{ "--encoding" }, // missing argument
				{ "--keyfile" }, // missing argument
				{ "--tofile" }, // missing argument
				{ "--base" }, // missing argument
				{ "--host" }, // missing argument
				{ "--connector" }, // missing argument
				{ "--connector", "fantasy.name" }, // invalid connector name
				{ "--key", "funk", "--hex" }, // non-hexadecimal key with --hex
				{ "--key", "babed", "--hex" }, // key of uneven length with --hex
				{ "--key", "pfunk", "--encoding", "fantasy string" }, // invalid encoding
				{ "--key", "k\u00eby", "--ascii"}, // non-ascii key with --ascii. Note that this drops the msb of the ë and hance uses "key" as the key -> 404
				{ "--key", "key with blanks", "--url"}, // url-encoded key may not contain blanks
				{ "--key", "cat", "--base", "http://localhost" + prefix + path }, // explicit but port is missing -> connection refused
				{ "--key", "cat", "--base", "http://localhost:" + port + "/gataca" + path }, // explicit but wrong base -> 404
				{ "--key", "cat", "--host", "my.fantasy.hostname" }, // bad hostname -> 404
				{ "--host", "localhost" }, // no key given
		};

		// test each good combination
		for (String[] args : goodArgsList) {
			Assert.assertNotNull(GetValueByKey.getValue(args, configuration));
		}
		// test each bad combination
		for (String[] args : badArgsList) {
			Assert.assertNull(GetValueByKey.getValue(args, configuration));
		}

		// and shut down
		gateway.stop();
	}
}

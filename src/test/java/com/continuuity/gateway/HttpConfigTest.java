package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.util.HttpConfig;
import org.junit.Assert;
import org.junit.Test;

public class HttpConfigTest {

	/** verify that config picks up the port, path and other options from config	*/
	@Test
	public void testConfiguration() throws Exception {
		String name = "restful"; // use a different name

		// test default configuration
		HttpConfig defaults = new HttpConfig(name);
		Assert.assertEquals(name, defaults.getName());
		Assert.assertEquals(HttpConfig.DefaultPath, defaults.getPathMiddle());
		Assert.assertEquals(HttpConfig.DefaultPrefix, defaults.getPathPrefix());
		Assert.assertEquals(HttpConfig.DefaultPort, defaults.getPort());
		Assert.assertEquals(HttpConfig.DefaultSsl, defaults.isSsl());
		Assert.assertEquals(HttpConfig.DefaultChunking, defaults.isChunking());
		Assert.assertEquals(HttpConfig.DefaultMaxContentSize, defaults.getMaxContentSize());

		// test that setters work
		int port = 1234;
		String prefix = "/continewity";
		String path = "/pathway/";
		defaults.setPort(port)
				.setPrefix(prefix)
				.setPath(path);
		Assert.assertEquals(name, defaults.getName());
		Assert.assertEquals(path, defaults.getPathMiddle());
		Assert.assertEquals(prefix, defaults.getPathPrefix());
		Assert.assertEquals(port, defaults.getPort());
		Assert.assertEquals(HttpConfig.DefaultSsl, defaults.isSsl());
		Assert.assertEquals(HttpConfig.DefaultChunking, defaults.isChunking());
		Assert.assertEquals(HttpConfig.DefaultMaxContentSize, defaults.getMaxContentSize());

		// test that defaults carry over
		name = "rusty";
		CConfiguration configuration = new CConfiguration();
		HttpConfig config = HttpConfig.configure(name, configuration, defaults);
		Assert.assertEquals(name, config.getName());
		Assert.assertEquals(path, config.getPathMiddle());
		Assert.assertEquals(prefix, config.getPathPrefix());
		Assert.assertEquals(port, config.getPort());
		Assert.assertEquals(HttpConfig.DefaultSsl, config.isSsl());
		Assert.assertEquals(HttpConfig.DefaultChunking, config.isChunking());
		Assert.assertEquals(HttpConfig.DefaultMaxContentSize, config.getMaxContentSize());

		// test that configure() picks up all parameters
		name = "resty";
		port = 5555;
		prefix = "/continuuity";
		path = "/destination/";
		int maxSize = 2 * HttpConfig.DefaultMaxContentSize;
		configuration = new CConfiguration();
		configuration.setInt(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
		configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), path);
		configuration.setBoolean(Constants.buildConnectorPropertyName(name, Constants.CONFIG_CHUNKING), !HttpConfig.DefaultChunking);
		configuration.setBoolean(Constants.buildConnectorPropertyName(name, Constants.CONFIG_SSL), true);
		configuration.setInt(Constants.buildConnectorPropertyName(name, Constants.CONFIG_MAX_SIZE), maxSize);
		config = HttpConfig.configure(name, configuration, defaults);
		Assert.assertEquals(port, config.getPort());
		Assert.assertEquals(prefix, config.getPathPrefix());
		Assert.assertEquals(path, config.getPathMiddle());
		Assert.assertEquals(!HttpConfig.DefaultChunking, config.isChunking());
		Assert.assertEquals(maxSize, config.getMaxContentSize());
		Assert.assertFalse(config.isSsl());
	}

}

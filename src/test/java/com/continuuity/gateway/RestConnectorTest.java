package com.continuuity.gateway;

import com.continuuity.gateway.connector.rest.RestConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Test;

public class RestConnectorTest {

	static RestConnector newConnector(String name) {
		RestConnector connector = new RestConnector();
		connector.setName(name);
		return connector;
	}

	/** verify that connector picks up the port, path and other options from config	*/
	@Test
	public void testConfiguration() throws Exception {
		String name = "restful"; // use a different name

		Configuration configuration = new Configuration();
		RestConnector connector = newConnector(name);
		connector.configure(configuration);
		Assert.assertEquals(RestConnector.DefaultPort, connector.getPort());
		Assert.assertEquals(RestConnector.DefaultPrefix, connector.getPrefix());
		Assert.assertEquals(RestConnector.DefaultPath, connector.getPath());
		Assert.assertEquals(RestConnector.DefaultChunking, connector.isChunking());
		Assert.assertFalse(connector.isSsl()); // not yet implemented

		name = "resty";
		int port = 5555;
		String prefix = "/continuuity";
		String path = "/destination/";
		configuration.setInt(Constants.connectorConfigName(name, Constants.CONFIG_PORTNUMBER), port);
		configuration.set(Constants.connectorConfigName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.connectorConfigName(name, Constants.CONFIG_PATH_STREAM), path);
		configuration.setBoolean(Constants.connectorConfigName(name, Constants.CONFIG_CHUNKING), !RestConnector.DefaultChunking);
		configuration.setBoolean(Constants.connectorConfigName(name, Constants.CONFIG_SSL), false);
		connector = newConnector(name);
		connector.configure(configuration);
		Assert.assertEquals(port, connector.getPort());
		Assert.assertEquals(prefix, connector.getPrefix());
		Assert.assertEquals(path, connector.getPath());
		Assert.assertEquals(!RestConnector.DefaultChunking, connector.isChunking());
		Assert.assertFalse(connector.isSsl());
	}

	/** verify that connector does not bind to port until start() */
	@Test
	public void testStartStop() throws Exception {
		String name = "rusty";
		String prefix = "/continuuity";
		String path = "/q/";
		String stream = "pfunk";
		int port = Util.findFreePort();
		// configure connector but don't start
		Configuration configuration = new Configuration();
		configuration.setInt(Constants.connectorConfigName(name, Constants.CONFIG_PORTNUMBER), port);
		configuration.set(Constants.connectorConfigName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.connectorConfigName(name, Constants.CONFIG_PATH_STREAM), path);
		Connector connector = newConnector(name);
		connector.configure(configuration);
		connector.setConsumer(new Util.NoopConsumer());
		// create an http post
		HttpPost post = Util.createHttpPost(port, prefix, path, stream, 42);
		try { // verify send fails before start()
			Util.sendRestEvent(post);
			Assert.fail("Exception expected when connector has not started");
		} catch (Exception e) {	}
		connector.start();
		// send should now succeed
		Util.sendRestEvent(post);
		connector.stop();
		try { // verify send fails after stop
			Util.sendRestEvent(post);
			Assert.fail("Exception expected when connector has not started");
		} catch (Exception e) {	}
		connector.start();
		// after restart it should succeed again
		Util.sendRestEvent(post);
		connector.stop();
	}

	/** verify that rest events get transformed and annotated correctly */
	@Test
	public void testTransformEvent() throws Exception {
		String name = "other";
		String prefix = "/data";
		String path = "/stream/";
		String stream = "foo";
		int eventsToSend = 10;
		int port = Util.findFreePort();
		Configuration configuration = new Configuration();
		configuration.setInt(Constants.connectorConfigName(name, Constants.CONFIG_PORTNUMBER), port);
		configuration.set(Constants.connectorConfigName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.connectorConfigName(name, Constants.CONFIG_PATH_STREAM), path);
		Connector connector = newConnector(name);
		connector.configure(configuration);
		connector.setConsumer(new Util.VerifyConsumer(17, name, stream));
		connector.start();
		Util.sendRestEvent(Util.createHttpPost(port, prefix, path, stream, 17));
		connector.stop();
		connector.setConsumer(new Util.VerifyConsumer(name, stream));
		connector.start();
		Util.sendRestEvents(port, prefix, path, stream, eventsToSend);
		connector.stop();
		Assert.assertEquals(eventsToSend, connector.getConsumer().eventsReceived());
		Assert.assertEquals(eventsToSend, connector.getConsumer().eventsSucceeded());
		Assert.assertEquals(0, connector.getConsumer().eventsFailed());
	}
}

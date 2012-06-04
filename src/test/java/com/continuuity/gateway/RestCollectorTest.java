package com.continuuity.gateway;

import com.continuuity.gateway.collector.rest.RestCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Test;

public class RestCollectorTest {

	static RestCollector newCollector(String name) {
		RestCollector collector = new RestCollector();
		collector.setName(name);
		return collector;
	}

	/** verify that collector picks up the port, path and other options from config	*/
	@Test
	public void testConfiguration() throws Exception {
		String name = "restful"; // use a different name

		Configuration configuration = new Configuration();
		RestCollector collector = newCollector(name);
		collector.configure(configuration);
		Assert.assertEquals(RestCollector.DefaultPort, collector.getPort());
		Assert.assertEquals(RestCollector.DefaultPrefix, collector.getPrefix());
		Assert.assertEquals(RestCollector.DefaultPath, collector.getPath());
		Assert.assertEquals(RestCollector.DefaultChunking, collector.isChunking());
		Assert.assertFalse(collector.isSsl()); // not yet implemented

		name = "resty";
		int port = 5555;
		String prefix = "/continuuity";
		String path = "/destination/";
		configuration.setInt(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PORT), port);
		configuration.set(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PATH_STREAM), path);
		configuration.setBoolean(Constants.buildCollectorPropertyName(name, Constants.CONFIG_CHUNKING), !RestCollector.DefaultChunking);
		configuration.setBoolean(Constants.buildCollectorPropertyName(name, Constants.CONFIG_SSL), false);
		collector = newCollector(name);
		collector.configure(configuration);
		Assert.assertEquals(port, collector.getPort());
		Assert.assertEquals(prefix, collector.getPrefix());
		Assert.assertEquals(path, collector.getPath());
		Assert.assertEquals(!RestCollector.DefaultChunking, collector.isChunking());
		Assert.assertFalse(collector.isSsl());
	}

	/** verify that collector does not bind to port until start() */
	@Test
	public void testStartStop() throws Exception {
		String name = "rusty";
		String prefix = "/continuuity";
		String path = "/q/";
		String stream = "pfunk";
		int port = Util.findFreePort();
		// configure collector but don't start
		Configuration configuration = new Configuration();
		configuration.setInt(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PORT), port);
		configuration.set(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PATH_STREAM), path);
		Collector collector = newCollector(name);
		collector.configure(configuration);
		collector.setConsumer(new Util.NoopConsumer());
		// create an http post
		HttpPost post = Util.createHttpPost(port, prefix, path, stream, 42);
		try { // verify send fails before start()
			Util.sendRestEvent(post);
			Assert.fail("Exception expected when collector has not started");
		} catch (Exception e) {	}
		collector.start();
		// send should now succeed
		Util.sendRestEvent(post);
		collector.stop();
		try { // verify send fails after stop
			Util.sendRestEvent(post);
			Assert.fail("Exception expected when collector has not started");
		} catch (Exception e) {	}
		collector.start();
		// after restart it should succeed again
		Util.sendRestEvent(post);
		collector.stop();
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
		configuration.setInt(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PORT), port);
		configuration.set(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PATH_STREAM), path);
		Collector collector = newCollector(name);
		collector.configure(configuration);
		collector.setConsumer(new Util.VerifyConsumer(17, name, stream));
		collector.start();
		Util.sendRestEvent(Util.createHttpPost(port, prefix, path, stream, 17));
		collector.stop();
		collector.setConsumer(new Util.VerifyConsumer(name, stream));
		collector.start();
		Util.sendRestEvents(port, prefix, path, stream, eventsToSend);
		collector.stop();
		Assert.assertEquals(eventsToSend, collector.getConsumer().eventsReceived());
		Assert.assertEquals(eventsToSend, collector.getConsumer().eventsSucceeded());
		Assert.assertEquals(0, collector.getConsumer().eventsFailed());
	}
}

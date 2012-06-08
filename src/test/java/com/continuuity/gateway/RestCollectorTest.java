package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.collector.RestCollector;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Test;

public class RestCollectorTest {

	static RestCollector newCollector(String name) {
		RestCollector collector = new RestCollector();
		collector.setName(name);
		return collector;
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
		CConfiguration configuration = new CConfiguration();
		configuration.setInt(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
		configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), path);
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
		CConfiguration configuration = new CConfiguration();
		configuration.setInt(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
		configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), path);
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

	/** This tests that the collector returns the correct HTTP codes for invalid requests */
	@Test
	public void testBadRequests() throws Exception {
		// configure an collector
		final String name = "collect.rest";
		final String prefix = "/continuuity";
		final String path = "/stream/";
		final int port = Util.findFreePort();

		CConfiguration configuration = new CConfiguration();
		configuration.set(Constants.CONFIG_CONNECTORS, name);
		configuration.set(Constants.buildConnectorPropertyName(name,
				Constants.CONFIG_CLASSNAME), RestCollector.class.getCanonicalName());
		configuration.setInt(Constants.buildConnectorPropertyName(name,
				Constants.CONFIG_PORT),port);
		configuration.set(Constants.buildConnectorPropertyName(name,
				Constants.CONFIG_PATH_PREFIX), prefix);
		configuration.set(Constants.buildConnectorPropertyName(name,
				Constants.CONFIG_PATH_MIDDLE), path);

		// create, configure, and start Accessor
		RestCollector collector = new RestCollector();
		collector.setName(name);
		collector.setConsumer(new Util.NoopConsumer());
		collector.configure(configuration);
		collector.start();

		// the correct URL would be http://localhost:<port>/continuuity/stream/
		String baseUrl = collector.getHttpConfig().getBaseUrl();

		// submit a request without prefix in the path -> 404 Not Found
		Assert.assertEquals(404, Util.sendPostRequest("http://localhost:" + port + "/somewhere"));
		Assert.assertEquals(404, Util.sendPostRequest("http://localhost:" + port + "/continuuity/data"));

		// submit a request with correct prefix but no stream -> 404 Not Found
		Assert.assertEquals(404, Util.sendPostRequest(baseUrl));

		// submit a GET to the collector (which only supports POST) -> 405 Not Allowed
		Assert.assertEquals(405, Util.sendGetRequest(baseUrl));

		// submit a POST with stream name but more after that in the path -> 404 Not Found
		Assert.assertEquals(404, Util.sendPostRequest(baseUrl + "events/"));
		Assert.assertEquals(404, Util.sendPostRequest(baseUrl + "events/more"));

		// submit a POST with existing key but with query part -> 501 Not Implemented
		Assert.assertEquals(501, Util.sendPostRequest(baseUrl + "x?query=none"));

		// and shutdown
		collector.stop();
	}
}

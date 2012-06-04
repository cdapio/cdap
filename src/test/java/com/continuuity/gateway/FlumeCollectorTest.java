package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.collector.flume.FlumeCollector;
import com.continuuity.gateway.collector.flume.NettyFlumeCollector;
import org.apache.flume.event.SimpleEvent;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class FlumeCollectorTest {

	static FlumeCollector newCollector(String name) {
		FlumeCollector collector = new NettyFlumeCollector();
		collector.setName(name);
		return collector;
	}

	/** verify that collector picks up the port from config	*/
	@Test
	public void testConfiguration() throws Exception {
		String name = "notflume"; // use a different name

    Configuration configuration = CConfiguration.create();
    FlumeCollector collector = newCollector(name);
		collector.configure(configuration);
		Assert.assertEquals(FlumeCollector.DefaultPort, collector.getPort());

		name = "otherflume";
		int port = 9000;
		configuration.setInt(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PORT), port);
		collector = newCollector(name);
		collector.configure(configuration);
		Assert.assertEquals(port, collector.getPort());
	}

	/** verify that collector does not bind to port until start() */
	@Test
	public void testStartStop() throws Exception {
		String name = "other";
		int port = Util.findFreePort();
		String stream = "pfunk";
		// configure collector but don't start
		Configuration configuration = CConfiguration.create();
		configuration.setInt(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PORT), port);
		Collector collector = newCollector(name);
		collector.configure(configuration);
		collector.setConsumer(new Util.NoopConsumer());
		// create an event to reuse
		SimpleEvent event = Util.createFlumeEvent(42, stream);
		try { // verify send fails before start()
			Util.sendFlumeEvent(port, event);
			Assert.fail("Exception expected when collector has not started");
		} catch (Exception e) {	}
		collector.start();
		// send should now succeed
		Util.sendFlumeEvent(port, event);
		collector.stop();
		try { // verify send fails after stop
			Util.sendFlumeEvent(port, event);
			Assert.fail("Exception expected when collector has not started");
		} catch (Exception e) {	}
		collector.start();
		// after restart it should succeed again
		Util.sendFlumeEvent(port, event);
		collector.stop();
	}

	/** verify that flume events get transformed and annotated correctly */
	@Test
	public void testTransformEvent() throws Exception {
		String name = "other";
		int port = Util.findFreePort();
		String stream = "foo";
		int eventsToSend = 10;
    Configuration configuration = CConfiguration.create();
    configuration.setInt(Constants.buildCollectorPropertyName(name, Constants.CONFIG_PORT), port);
		Collector collector = newCollector(name);
		collector.configure(configuration);
		collector.setConsumer(new Util.VerifyConsumer(17, name, stream));
		collector.start();
		Util.sendFlumeEvent(port, Util.createFlumeEvent(17, stream));
		collector.stop();
		collector.setConsumer(new Util.VerifyConsumer(name, stream));
		collector.start();
		Util.sendFlumeEvents(port, stream, eventsToSend, 4);
		collector.stop();
		Assert.assertEquals(eventsToSend, collector.getConsumer().eventsReceived());
		Assert.assertEquals(eventsToSend, collector.getConsumer().eventsSucceeded());
		Assert.assertEquals(0, collector.getConsumer().eventsFailed());
	}
}

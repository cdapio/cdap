package com.continuuity.gateway;

import com.continuuity.gateway.connector.flume.FlumeConnector;
import com.continuuity.gateway.connector.flume.NettyFlumeConnector;
import org.apache.flume.event.SimpleEvent;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class FlumeConnectorTest {

	static FlumeConnector newConnector(String name) {
		FlumeConnector connector = new NettyFlumeConnector();
		connector.setName(name);
		return connector;
	}

	/** verify that connector picks up the port from config	*/
	@Test
	public void testConfiguration() throws Exception {
		String name = "notflume"; // use a different name

		Configuration configuration = new Configuration();
		FlumeConnector connector = newConnector(name);
		connector.configure(configuration);
		Assert.assertEquals(FlumeConnector.DefaultPort, connector.getPort());

		name = "otherflume";
		int port = 9000;
		configuration.setInt(Constants.connectorConfigName(name, Constants.CONFIG_PORTNUMBER), port);
		connector = newConnector(name);
		connector.configure(configuration);
		Assert.assertEquals(port, connector.getPort());
	}

	/** verify that connector does not bind to port until start() */
	@Test
	public void testStartStop() throws Exception {
		String name = "other";
		int port = Util.findFreePort();
		String stream = "pfunk";
		// configure connector but don't start
		Configuration configuration = new Configuration();
		configuration.setInt(Constants.connectorConfigName(name, Constants.CONFIG_PORTNUMBER), port);
		Connector connector = newConnector(name);
		connector.configure(configuration);
		connector.setConsumer(new Util.NoopConsumer());
		// create an event to reuse
		SimpleEvent event = Util.createFlumeEvent(42, stream);
		try { // verify send fails before start()
			Util.sendFlumeEvent(port, event);
			Assert.fail("Exception expected when connector has not started");
		} catch (Exception e) {	}
		connector.start();
		// send should now succeed
		Util.sendFlumeEvent(port, event);
		connector.stop();
		try { // verify send fails after stop
			Util.sendFlumeEvent(port, event);
			Assert.fail("Exception expected when connector has not started");
		} catch (Exception e) {	}
		connector.start();
		// after restart it should succeed again
		Util.sendFlumeEvent(port, event);
		connector.stop();
	}

	/** verify that flume events get transformed and annotated correctly */
	@Test
	public void testTransformEvent() throws Exception {
		String name = "other";
		int port = Util.findFreePort();
		String stream = "foo";
		int eventsToSend = 10;
		Configuration configuration = new Configuration();
		configuration.setInt(Constants.connectorConfigName(name, Constants.CONFIG_PORTNUMBER), port);
		Connector connector = newConnector(name);
		connector.configure(configuration);
		connector.setConsumer(new Util.VerifyConsumer(17, name, stream));
		connector.start();
		Util.sendFlumeEvent(port, Util.createFlumeEvent(17, stream));
		connector.stop();
		connector.setConsumer(new Util.VerifyConsumer(name, stream));
		connector.start();
		Util.sendFlumeEvents(port, stream, eventsToSend, 4);
		connector.stop();
		Assert.assertEquals(eventsToSend, connector.getConsumer().eventsReceived());
		Assert.assertEquals(eventsToSend, connector.getConsumer().eventsSucceeded());
		Assert.assertEquals(0, connector.getConsumer().eventsFailed());
	}
}

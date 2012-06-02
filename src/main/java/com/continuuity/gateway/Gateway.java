package com.continuuity.gateway;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The Gateway provides a data interface for external clients to the data fabric
 * <ul>
 *   <li>To send events to a named queue. This is supported via various protocols. Every protocol
 *   is implemented as a Connector that it registered with the Gateway. The connector can implement
 *   any protocol, as long as it can convert the data from that protocol into events. All events are
 *   routed to a Consumer that is also registered with the gateway. The consumer is responsible for
 *   persisting the event before returning a response. </li>
 *   <li>To read data from the persistence storage</li>
 * </ul>
 */
public class Gateway {

	private static final Logger LOG = LoggerFactory
			.getLogger(Gateway.class);

	private List<Connector> connectors = new ArrayList<Connector>();
	private Configuration configuration;
	private Consumer consumer = null;

	/**
	 * Set the gateway's configuration, the create and configure the connectors.
	 * @param configuration the configuration object that contains the option
	 *                      for the gateway and all its connectors
	 */
	public void configure(Configuration configuration) {
		this.configuration = configuration;
		Collection<String> connectorNames = configuration.
				getStringCollection(Constants.CONFIG_CONNECTORS);
		for (String connectorName : connectorNames) {
			String configName = Constants.connectorConfigName(connectorName, Constants.CONFIG_CLASSNAME);
			String connectorClassName = configuration.get(configName);
			if (connectorClassName == null) {
				LOG.error("Required property '" + configName +
						"' missing. Skipping connector '" + connectorName + "'.");
				continue;
			}
			Connector connector = null;
			try {
				connector = (Connector)Class.forName(connectorClassName).newInstance();
				connector.setName(connectorName);
			} catch (Exception e) {
				LOG.error("Cannot instantiate class " + connectorClassName + "(" +
						e.getMessage() + "). Skipping connector '" + connectorName + "'.");
				continue;
			}
			try {
				connector.configure(configuration);
			} catch (Exception e) {
				LOG.error("Error configuring connector '" + connectorName + "' (" +
						e.getMessage() + "). Skipping connector '" + connectorName + "'.");
				continue;
			}
			this.connectors.add(connector);
		}
	}

	/**
	 * Get the gateway's configuration
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return this.configuration;
	}

	/**
	 * Add a connector to the gateway. This connector must be in pristine state and not started yet.
	 * The gateway will start the connector when it starts itself.
	 * @param connector The connector to register
	 * @throws Exception iff a connector with the same name is already registered
	 */
	public void addConnector(Connector connector) throws Exception {
		String name = connector.getName();
		if (name == null) {
			Exception e = new IllegalArgumentException("Connector name cannot be null.");
			LOG.error(e.getMessage());
			throw e;
		}
		LOG.info("Adding connector '" + name + "' of type " + connector.getClass().getName() + ".");
		if (this.hasNamedConnector(name)) {
			Exception e = new Exception("Connector with name '" + name + "' already registered. ");
			LOG.error(e.getMessage());
			throw e;
		} else {
			this.connectors.add(connector);
		}
	}

	/**
	 *  Set the consumer that all events are routed to. This should be called before the
	 *  Gateway is started. Upon start(), the gateway will call startConsumer on the
	 *  consumer.
	 *  @param consumer The consumer that all events will be sent to
	 */
	public void setConsumer(Consumer consumer) {
		LOG.info("Setting consumer to " + consumer.getClass().getName() + ".");
		for (Connector connector : this.connectors) {
			connector.setConsumer(consumer);
		}
		this.consumer = consumer;
	}

	/**
	 * check whether a connector with the given name is already registered
	 * @param name the name to be checked
	 * @return true iff a connector with the same name exists
	 */
	private boolean hasNamedConnector(String name) {
		for (Connector connector : this.connectors) {
			if (connector.getName().equals(name))
				return true;
		}
		return false;
	}

	/**
	 * Start the gateway. This will also start the consumer and all the connectors.
	 * @throws Exception if there is no consumer, or whatever exception a connector
	 * throws during start().
	 */
	public void start() throws Exception {
		LOG.info("Starting up.");
		if (this.consumer == null) {
			Exception e = new Exception("Cannot start gateway without consumer.");
			LOG.error(e.getMessage());
			throw e;
		}
		this.consumer.startConsumer();
		for (Connector connector : this.connectors) {
			connector.start();
		}
	}

	/**
	 * Stop the gateway. This will first stop all connectors, and the stop the consumer.
	 */
	public void stop() throws Exception {
		LOG.info("Shutting down.");
		for (Connector connector : this.connectors) {
			connector.stop();
		}
		this.consumer.stopConsumer();
	}
}

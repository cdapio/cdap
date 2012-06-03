package com.continuuity.gateway;

import com.google.inject.Inject;
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
 *   routed to a Consumer that is also registered with the gateway. The theConsumer is responsible for
 *   persisting the event before returning a response. </li>
 *   <li>To read data from the persistence storage</li>
 * </ul>
 */
public class Gateway {

  /**
   * This is our Logger instance
   */
	private static final Logger LOG = LoggerFactory.getLogger(Gateway.class);

  /**
   * The list of Connectors for this Gateway
   */
	private List<Connector> connectorList = new ArrayList<Connector>();

  /**
   * Our Configuration object
   *
   * TODO: Figure out a way to populate this from an external file
   */
	private Configuration myConfiguration;

  /**
   * This is the Consumer that we'll be using. Gateway can not function without
   * a valid Consumer.
   */
  @Inject
  private Consumer theConsumer;

	/**
	 * Set the gateway's Configuration, then create and configure the Connectors
   *
	 * @param configuration The Configuration object that contains the options
	 *                      for the Gateway and all its Connectors
	 */
	public void configure(Configuration configuration) {

    // Save the configuration
		myConfiguration = configuration;

		Collection<String> connectorNames = configuration.
				getStringCollection(Constants.CONFIG_CONNECTORS);

    for (String connectorName : connectorNames) {

			String configName = Constants.connectorConfigName
          (connectorName, Constants.CONFIG_CLASSNAME);

			String connectorClassName = configuration.get(configName);

			if (connectorClassName == null) {
				LOG.error("Required property '" + configName +
						"' missing. Skipping connector '" + connectorName + "'.");
				continue;
			}

      // Instantiate a new Connector and then configure it
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

      // Add it to our list
			connectorList.add(connector);
		}
	}

  /**
   * Get the Gateway's current configuration
   * @return Our current Configuration
   */
  public Configuration getConfiguration() {
    return myConfiguration;
  }

  /**
   * Start the gateway. This will also start the Consumer and all the Connectors
   *
   * @throws Exception If there is no Consumer, or whatever Exception a
   * connector throws during start().
   */
  public void start() throws Exception {

    // Check we are in the correct state
    if (theConsumer == null) {
      Exception e = new Exception("Cannot start Gateway without a Consumer.");
      LOG.error(e.getMessage());
      throw e;
    }

    LOG.info("Gateway Starting up.");
    theConsumer.startConsumer();

    // Now start all our Connectors
    for (Connector connector : this.connectorList) {

      // First, set the Consumer for the Connector
      // TODO: This should probably be done in the addConnector method?
      connector.setConsumer(theConsumer);

      connector.start();

      LOG.info(" Starting " + connector.getName() + " connector");
    }
  }

  /**
   * Stop the gateway. This will first stop all our Connectors, and then stop
   * the Consumer.
   */
  public void stop() throws Exception {

    LOG.info("Gateway Shutting down");

    // Stop all our connectors
    for (Connector connector : this.connectorList) {
      connector.stop();
      LOG.info(" " + connector.getName() + " stopped");
    }

    // Stop the consumer
    theConsumer.stopConsumer();
    LOG.info(" Consumer stopped");

    LOG.info("Gateway successfully shut down");

  }

	/**
	 * Add a Connector to the Gateway. This connector must be in pristine state
   * and not started yet. The Gateway will start the connector when it starts
   * itself.
   *
	 * @param connector The connector to register
	 * @throws Exception iff a connector with the same name is already registered
	 */
	public void addConnector(Connector connector) throws Exception {

		String name = connector.getName();
		if (name == null) {
			Exception e =
          new IllegalArgumentException("Connector name cannot be null.");
			LOG.error(e.getMessage());
			throw e;
		}

    LOG.info("Adding connector '" + name + "' of type " +
        connector.getClass().getName() + ".");

    if (this.hasNamedConnector(name)) {
			Exception e = new Exception("Connector with name '" + name
          + "' already registered. ");
			LOG.error(e.getMessage());
			throw e;
		} else {
			connectorList.add(connector);
		}
	}

	/**
	 *  Set the Consumer that all events are routed to.
   *
	 *  @param consumer The Consumer that all events will be sent to
   *
   *  @throws IllegalArgumentException If the consumer object is null
	 */
	public void setConsumer(Consumer consumer) {

    // Check our pre conditions
    if (consumer == null) {
      throw new IllegalArgumentException("'consumer' argument was null");
    }

		LOG.info("Setting Consumer to " + consumer.getClass().getName() + ".");
    theConsumer = consumer;

	}

	/**
	 * Check whether a connector with the given name is already registered
   *
	 * @param name The name to be checked
	 * @return true If a connector with the same name exists
	 */
	private boolean hasNamedConnector(String name) {
		for (Connector connector : this.connectorList) {
			if (connector.getName().equals(name))
				return true;
		}
		return false;
	}


} // end of Gateway class

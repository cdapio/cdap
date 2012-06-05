package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The Gateway is the front door to the Continuuity platform. It provides a
 * data interface for external clients to send events to the data fabric.
 * It supports two main patterns:
 * <dl>
 *   <dt><strong>Send events to a named queue</strong></dt>
 *   <dd>This is supported via various protocols. Every protocol is implemented
 *   as a Collector that it registered with the Gateway. The collector can
 *   implement any protocol, as long as it can convert the data from that
 *   protocol into events. All events are routed to a Consumer that is also
 *   registered with the gateway. The Consumer is responsible for persisting
 *   the event before returning a response.</dd>
 *   <dt><strong>To read data from the data fabric</strong></dt>
 *   <dd>This is currently not implemented.</dd>
 * </dl>
 */
public class Gateway {

  /**
   * This is our Logger instance
   */
	private static final Logger LOG = LoggerFactory.getLogger(Gateway.class);

	/**
	 * This is the consumer that all collectors will use.
	 * Gateway can not function without a valid Consumer.
	 */
	@Inject
	private Consumer consumer;

	/**
	 * This is the executor that all accessors will use for the data fabric.
	 * Gateway can not function withou a valid operation executor.
	 */
	@Inject
	private OperationExecutor executor;

	/**
   * The list of connectors for this Gateway. This list is populated in
   * the configure method.
   */
	private List<Connector> connectorList = new ArrayList<Connector>();

  /**
   * Our Configuration object
   *
   * TODO: Figure out a way to populate this from an external file
   */
	private CConfiguration myConfiguration;

	/**
	 * Get the Gateway's current configuration
	 * @return Our current Configuration
	 */
	public CConfiguration getConfiguration() {
		return myConfiguration;
	}

	/**
	 * Set the gateway's Configuration, then create and configure the connectors
   *
	 * @param configuration The Configuration object that contains the options
	 *                      for the Gateway and all its connectors. This can not
   *                      be null.
   *
   * @throws IllegalArgumentException If configuration argument is null.
	 */
	public void configure(CConfiguration configuration) {

    if (configuration == null) {
      throw new IllegalArgumentException("'configuration' argument was null");
    }

    LOG.info("Configuring Gateway..");

    // Save the configuration so we can use it again later
		myConfiguration = configuration;

    // Retrieve the list of connectors that we will create
		Collection<String> connectorNames = myConfiguration.
				getStringCollection(Constants.CONFIG_CONNECTORS);

    // For each Connector
    for (String connectorName : connectorNames) {

      // Retrieve the connector's Class
			String connectorClassName = myConfiguration.get(
          Constants.buildConnectorPropertyName(connectorName,
							Constants.CONFIG_CLASSNAME));
      // Has the user specified the Class? If not, skip this Connector
			if (connectorClassName == null) {
        LOG.error("No Class property defined for " + connectorName +
            ". Can not create " + connectorName + ".");
			} else {
        // Instantiate a new Connector and then configure it
        Connector newConnector;
        try {
          // Attempt to load the Class
          newConnector =
              (Connector)Class.forName(connectorClassName).newInstance();
          // Tell it what it's called
          newConnector.setName(connectorName);

        } catch (Exception e) {
          LOG.error("Cannot instantiate class " + connectorClassName + "(" +
              e.getMessage() + "). Skipping Connector '" + connectorName + "'.");
          continue;
        }

        // Now try to configure the Connector
        try {
          newConnector.configure(myConfiguration);
        } catch (Exception e) {
          LOG.error("Error configuring connector '" + connectorName + "' (" +
              e.getMessage() + "). Skipping connector '" + connectorName + "'.");
          continue;
        }

        // Add it to our Connector list
				try {
	        this.addConnector(newConnector);
				} catch (Exception e) {
					LOG.error("Error adding connector '" + connectorName + "' (" +
							e.getMessage() + "). Skipping connector '" + connectorName + "'.");
					continue;
				}
      }
		}
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
   * Start the gateway. This will also start the Consumer and all the Connectors
   *
   * @throws Exception If there is no Consumer, or whatever Exception a
   * connector throws during start().
   */
  public void start() throws Exception {

    // Check we are in the correct state
		if (this.consumer == null) {
			Exception e = new Exception("Cannot start Gateway without a Consumer.");
			LOG.error(e.getMessage());
			throw e;
		}
		if (this.executor == null) {
			Exception e = new Exception("Cannot start Gateway without an Operation Executor.");
			LOG.error(e.getMessage());
			throw e;
		}

		LOG.info("Gateway Starting up.");

    // Start our event consumer
    this.consumer.startConsumer();

    // Now start all our Connectors
    for (Connector connector : this.connectorList) {

      // First, perform connector-type specific initialization
      // For a collector, set the Consumer for its events
			// For an accessor, set the operations executor for access to data fabric
      // TODO: This should probably be done in the addConnector method?
			if (connector instanceof Collector) {
	      ((Collector)connector).setConsumer(this.consumer);
			}
			else if (connector instanceof Accessor) {
				((Accessor)connector).setExecutor(this.executor);
			}
      connector.start();

      LOG.info(" Started " + connector.getName() + " connector");
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
    this.consumer.stopConsumer();
    LOG.info(" Consumer stopped");

    LOG.info("Gateway successfully shut down");

  }

	/**
	 *  Set the Consumer that all events are routed to.
	 *  @param consumer The Consumer that all events will be sent to
	 *  @throws IllegalArgumentException If the consumer object is null
	 */
	public void setConsumer(Consumer consumer) {
		if (consumer == null) {
			throw new IllegalArgumentException("'consumer' argument was null");
		}
		LOG.info("Setting Consumer to " + consumer.getClass().getName() + ".");
		this.consumer = consumer;
	}

	/**
	 *  Set the operations executor thatis used for all data fabric access.
	 *  @param executor The executor to use
	 *  @throws IllegalArgumentException If the consumer object is null
	 */
	public void setExecutor(OperationExecutor executor) {
		if (executor == null) {
			throw new IllegalArgumentException("'executor' argument was null");
		}
		LOG.info("Setting Operations Executor to " + executor.getClass().getName() + ".");
		this.executor = executor;
	}

	/**
	 * Check whether a connector with the given name is already registered
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

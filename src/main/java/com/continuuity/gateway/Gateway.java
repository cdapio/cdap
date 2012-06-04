package com.continuuity.gateway;

import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
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
   * This is the Consumer that we'll be using. Gateway can not function without
   * a valid Consumer.
   */
  @Inject
  private Consumer theConsumer;

  /**
   * The list of collectors for this Gateway. This list is populated in
   * the configure method.
   */
	private List<Collector> collectorList = new ArrayList<Collector>();

  /**
   * Our Configuration object
   *
   * TODO: Figure out a way to populate this from an external file
   */
	private Configuration myConfiguration;

	/**
	 * Get the Gateway's current configuration
	 * @return Our current Configuration
	 */
	public Configuration getConfiguration() {
		return myConfiguration;
	}

	/**
	 * Set the gateway's Configuration, then create and configure the collectors
   *
	 * @param configuration The Configuration object that contains the options
	 *                      for the Gateway and all its collectors. This can not
   *                      be null.
   *
   * @throws IllegalArgumentException If configuration argument is null.
	 */
	public void configure(Configuration configuration) {

    if (configuration == null) {
      throw new IllegalArgumentException("'configuration' argument was null");
    }

    LOG.info("Configuring Gateway..");

    // Save the configuration so we can use it again later
		myConfiguration = configuration;

    // Retrieve the list of collectors that we will create
		Collection<String> collectorNames = myConfiguration.
				getStringCollection(Constants.CONFIG_COLLECTORS);

    // For each Collector
    for (String collectorName : collectorNames) {

      // Retrieve the collector's Class
			String collectorClassName = myConfiguration.get(
          Constants.buildCollectorPropertyName(collectorName,
                                               Constants.CONFIG_CLASSNAME));

      // Has the user specified the Class? If not, skip this Collector
			if (collectorClassName == null) {
        LOG.error("No Class property defined for " + collectorName +
            ". Can not create " + collectorName + ".");
			} else {

        // Instantiate a new Collector and then configure it
        Collector newCollector = null;

        try {

          // Attempt to load the Class
          newCollector =
              (Collector)Class.forName(collectorClassName).newInstance();

          // Tell it what it's called
          newCollector.setName(collectorName);

        } catch (Exception e) {
          LOG.error("Cannot instantiate class " + collectorClassName + "(" +
              e.getMessage() + "). Skipping Collector '" + collectorName + "'.");
          continue;
        }

        // Now try to configure the Collector
        try {
          newCollector.configure(myConfiguration);
        } catch (Exception e) {
          LOG.error("Error configuring collector '" + collectorName + "' (" +
              e.getMessage() + "). Skipping collector '" + collectorName + "'.");
          continue;
        }

        // Add it to our Collector list
				try {
	        this.addCollector(newCollector);
				} catch (Exception e) {
					LOG.error("Error adding collector '" + collectorName + "' (" +
							e.getMessage() + "). Skipping collector '" + collectorName + "'.");
					continue;

				}
      }
		}
	}

	/**
	 * Add a Collector to the Gateway. This collector must be in pristine state
	 * and not started yet. The Gateway will start the collector when it starts
	 * itself.
	 *
	 * @param collector The collector to register
	 * @throws Exception iff a collector with the same name is already registered
	 */
	public void addCollector(Collector collector) throws Exception {

		String name = collector.getName();
		if (name == null) {
			Exception e =
					new IllegalArgumentException("Collector name cannot be null.");
			LOG.error(e.getMessage());
			throw e;
		}

		LOG.info("Adding collector '" + name + "' of type " +
				collector.getClass().getName() + ".");

		if (this.hasNamedCollector(name)) {
			Exception e = new Exception("Collector with name '" + name
					+ "' already registered. ");
			LOG.error(e.getMessage());
			throw e;
		} else {
			collectorList.add(collector);
		}
	}

  /**
   * Start the gateway. This will also start the Consumer and all the Collectors
   *
   * @throws Exception If there is no Consumer, or whatever Exception a
   * collector throws during start().
   */
  public void start() throws Exception {

    // Check we are in the correct state
    if (theConsumer == null) {
      Exception e = new Exception("Cannot start Gateway without a Consumer.");
      LOG.error(e.getMessage());
      throw e;
    }

    LOG.info("Gateway Starting up.");

    // Start our event consumer
    theConsumer.startConsumer();

    // Now start all our Collectors
    for (Collector collector : this.collectorList) {

      // First, set the Consumer for the Collector
      // TODO: This should probably be done in the addCollector method?
      collector.setConsumer(theConsumer);

      collector.start();

      LOG.info(" Started " + collector.getName() + " collector");
    }
  }

  /**
   * Stop the gateway. This will first stop all our Collectors, and then stop
   * the Consumer.
   */
  public void stop() throws Exception {

    LOG.info("Gateway Shutting down");

    // Stop all our collectors
    for (Collector collector : this.collectorList) {
      collector.stop();
      LOG.info(" " + collector.getName() + " stopped");
    }

    // Stop the consumer
    theConsumer.stopConsumer();
    LOG.info(" Consumer stopped");

    LOG.info("Gateway successfully shut down");

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
	 * Check whether a collector with the given name is already registered
   *
	 * @param name The name to be checked
	 * @return true If a collector with the same name exists
	 */
	private boolean hasNamedCollector(String name) {
		for (Collector collector : this.collectorList) {
			if (collector.getName().equals(name))
				return true;
		}
		return false;
	}


} // end of Gateway class

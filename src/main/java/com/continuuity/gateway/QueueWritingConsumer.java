/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryQueueTable;
import com.continuuity.flow.flowlet.api.Event;
import com.continuuity.flow.flowlet.impl.EventSerializer;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * QueueWritingConsumer is an implementation of Consumer that writes all
 * events to an instance of (Memory)QueueTable.
 */
public class QueueWritingConsumer extends Consumer {

  /**
   * This is our Logger class
   */
	private static final Logger LOG = LoggerFactory
      .getLogger(QueueWritingConsumer.class);

  /**
   * This is our QueueTable object.
   *
   * TODO: Replace this with a an interface and NOT a concrete class (when there
   * is one!)
   */
  @Inject
	MemoryQueueTable myQueues;

  /**
   * Our Configuration object
   */
	CConfiguration myConfiguration;

  /**
   * The setter for our QueueTable implementation.
   *
   * TODO: This method should be removed once we have Guice working for all
   * the tests too.
   *
   * @param queueTable  The new QueuesTable to use, can not be null.
   * @throws IllegalArgumentException If 'queueTable' argument is null.
   */
  public void setQueueTable(MemoryQueueTable queueTable) {

    // Check our pre conditions
    if (queueTable == null) {
      throw new IllegalArgumentException("queueTable argument is null");
    }

    // Assign it
    this.myQueues = queueTable;

  } // end of setQueueTable


  /**
   * Configure this Consumer with a 'Configuration' object.
   *
   * @param configuration  The previously formed myConfiguration object to use.
   */
	@Override
	public void configure(CConfiguration configuration) {

    // Copy the configuration object
		this.myConfiguration = configuration;

    // Do we have any queues set up yet?
		if (this.myQueues == null) {
			setQueueTable(new MemoryQueueTable());
		}

	} // end of configure


	@Override
	protected void single(Event event) throws Exception {
		EventSerializer serializer = new EventSerializer();
		byte[] bytes = serializer.serialize(event);
		if (bytes == null) {
			LOG.warn("Could not serialize event: " + event);
			throw new Exception("Could not serialize event: " + event);
		}
		String destination = event.getHeader(Constants.HEADER_DESTINATION_ENDPOINT);
		if (destination == null) destination = "default";
		this.myQueues.push(destination.getBytes(), bytes);
	}

	// @todo implement batch as transaction as soon as transactional myQueues are ready
}

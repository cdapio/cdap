/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway;

import com.continuuity.data.engine.SimpleQueueTable;
import com.continuuity.data.engine.memory.MemoryQueueTable;
import com.continuuity.flow.flowlet.api.Event;
import com.continuuity.flow.flowlet.impl.EventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class QueueWritingConsumer extends Consumer {

	private static final Logger LOG = LoggerFactory
			.getLogger(QueueWritingConsumer.class);

	SimpleQueueTable queues;
	Configuration configuration;

	public QueueWritingConsumer(SimpleQueueTable queues) {
		this.queues = queues;
	}

	@Override
	public void configure(Configuration configuration) {
		this.configuration = configuration;
		if (this.queues == null) {
			this.queues = new MemoryQueueTable();
		}
	}

	@Override
	protected void single(Event event) throws Exception {
		EventSerializer serializer = new EventSerializer();
		byte[] bytes = serializer.serialize(event);
		if (bytes == null) {
			LOG.warn("Could not serialize event: " + event);
			throw new Exception("Could not serialize event: " + event);
		}
		this.queues.push("default".getBytes(), bytes);
	}

	// @todo implement batch as transaction as soon as transactional queues are ready
}

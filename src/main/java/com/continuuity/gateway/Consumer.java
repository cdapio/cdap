/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway;

import com.continuuity.flow.flowlet.api.Event;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.util.LocaleServiceProviderPool;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public abstract class Consumer {

	private final Logger LOG = LoggerFactory
			.getLogger(this.getClass());

	private AtomicLong callsReceived = new AtomicLong();
	private AtomicLong callsSucceeded = new AtomicLong();
	private AtomicLong callsFailed = new AtomicLong();
	private AtomicLong eventsReceived = new AtomicLong();
	private AtomicLong eventsSucceeded = new AtomicLong();
	private AtomicLong eventsFailed = new AtomicLong();

	public long callsReceived() { return this.callsReceived.get(); }
	public long callsSucceeded() { return this.callsSucceeded.get(); }
	public long callsFailed() { return this.callsFailed.get(); }
	public long eventsReceived() { return this.eventsReceived.get(); }
	public long eventsSucceeded() { return this.eventsSucceeded.get(); }
	public long eventsFailed() { return this.eventsFailed.get(); }

	public void configure(Configuration configuration) { }

	protected void start() {	}
	protected void stop() { }

	protected abstract void single(Event event) throws Exception;

	protected void batch(List<Event> events) throws Exception {
		for (Event event : events) {
			this.single(event);
		}
	}

	final void startConsumer() {
		LOG.info("Starting up.");

		this.callsReceived.set(0L);
		this.callsSucceeded.set(0L);
		this.callsFailed.set(0L);
		this.eventsReceived.set(0L);
		this.eventsSucceeded.set(0L);
		this.eventsFailed.set(0L);

		this.start();
	}

	final void stopConsumer() {
		this.stop();
		LOG.info("Shutting down.");
		LOG.info("  Calls/Events Received : " + this.callsReceived + "/" + this.eventsReceived);
		LOG.info("  Calls/Events Succeeded: " + this.callsSucceeded + "/" + this.eventsSucceeded);
		LOG.info("  Calls/Events Failed:    " + this.callsFailed + "/" + this.eventsFailed);
	}

	final public void consumeEvent(Event event) throws Exception {
		this.callsReceived.incrementAndGet();
		this.eventsReceived.incrementAndGet();
		try {
			this.single(event);
		} catch (Exception e) {
			this.callsFailed.incrementAndGet();
			this.eventsFailed.incrementAndGet();
			throw e;
		}
		this.callsSucceeded.incrementAndGet();
		this.eventsSucceeded.incrementAndGet();
	}

	final public void consumeEvents(List<Event> events) throws Exception {
		this.callsReceived.incrementAndGet();
		this.eventsReceived.addAndGet(events.size());
		try {
			this.batch(events);
		} catch (Exception e) {
			this.callsFailed.incrementAndGet();
			this.eventsFailed.addAndGet(events.size());
			throw e;
		}
		this.callsSucceeded.incrementAndGet();
		this.eventsSucceeded.addAndGet(events.size());
	}

}

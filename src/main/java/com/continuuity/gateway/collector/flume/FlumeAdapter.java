/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway.collector.flume;

import com.continuuity.flow.flowlet.api.Event;
import com.continuuity.flow.flowlet.impl.EventBuilder;
import com.continuuity.gateway.Collector;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.Consumer;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This class serves as an intermediary between the Avro flume responder
 * and the event consumer. It receives (batches of) flume events, converts
 * them into Events and adds additional meta data (such as the collector
 * name) to the headers, then passes the events on to the consumer.
 */
class FlumeAdapter implements AvroSourceProtocol {

	private static final Logger LOG = LoggerFactory
			.getLogger(FlumeAdapter.class);

	private Consumer consumer;
	private Collector collector;

	/** prevent using the default constructor, to ensure the collector is always set */
	private FlumeAdapter() {
		LOG.error("Attempt to call default constructor.");
		throw new UnsupportedOperationException("Attempt to call default constructor for FlumeAdapter.");
	}

	/**
	 * Constructor ensures that the collector is always set
	 * @param collector the collector that this adapter belongs to
	 */

	public FlumeAdapter(Collector collector) {
		this.collector = collector;
	}

	/**
	 * Set the consumer for the output events
	 * @param consumer the consumer
	 */
	public void setConsumer(Consumer consumer) {
		this.consumer = consumer;
	}
	/**
	 * Get the consumer for the output events
	 * @return the consumer
	 */
	public Consumer getConsumer() {
		return this.consumer;
	}
	/**
	 * Get the collector that this adapter belongs to
	 * @return the collector
	 */
	public Collector getCollector() {
		return this.collector;
	}

	@Override
	/** called by the Avro Responder for each single event */
	public final Status append(AvroFlumeEvent event) {
		LOG.debug("Received event: " + event);
		try {
			this.consumer.consumeEvent(convertFlume2Event(event));
			return Status.OK;
		} catch (Exception e) {
			LOG.warn("Error consuming single event: " + e.getMessage());
			return Status.FAILED;
		}
	}

	@Override
	/** called by the Avro Responder for each batch of events */
	public final Status appendBatch(List<AvroFlumeEvent> events) {
		LOG.debug("Received batch: " + events);
		try {
			this.consumer.consumeEvents(convertFlume2Event(events));
			return Status.OK;
		} catch (Exception e) {
			LOG.warn("Error consuming batch of events: " + e.getMessage());
			return Status.FAILED;
		}
	}

	/**
	 * Converts a Flume event to am Event. This is a pure copy of the headers and body.
	 * In addition, the collector name header is set.
	 * @param flumeEvent the flume event to be converted
	 * @return the resulting event
	 */
	protected Event convertFlume2Event(AvroFlumeEvent flumeEvent) {
		EventBuilder builder = new EventBuilder();
		builder.setBody(flumeEvent.getBody().array());
		for (CharSequence header : flumeEvent.getHeaders().keySet()) {
			builder.setHeader(header.toString(), flumeEvent.getHeaders().get(header).toString());
		}
		builder.setHeader(Constants.HEADER_FROM_COLLECTOR, this.getCollector().getName());
		return builder.create();
	}

	/**
	 * Converts a batch of Flume event to a lis of Events, using @ref convertFlume2Event
	 * @param flumeEvents the flume events to be converted
	 * @return the resulting events
	 */
	protected List<Event> convertFlume2Event(List<AvroFlumeEvent> flumeEvents) {
		List<Event>	events = new ArrayList<Event>();
		for (AvroFlumeEvent flumeEvent : flumeEvents) {
			events.add(convertFlume2Event(flumeEvent));
		}
		return events;
	}
}

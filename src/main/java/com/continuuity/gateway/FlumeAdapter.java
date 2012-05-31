/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway;

import com.continuuity.flow.flowlet.api.Event;
import com.continuuity.flow.flowlet.impl.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by: andreas, having fun since April 2012
 */
class FlumeAdapter implements AvroSourceProtocol {

	private static final Logger LOG = LoggerFactory
			.getLogger(FlumeAdapter.class);

	private Consumer consumer;

	public void setConsumer(Consumer consumer) {
		this.consumer = consumer;
	}

	public Consumer getConsumer() {
		return this.consumer;
	}

	public Status consumeSingle(AvroFlumeEvent event) {
		try {
			this.consumer.consumeEvent(convertFlume2Event(event));
			return Status.OK;
		} catch (Exception e) {
			LOG.warn("Error consuming single event: " + e.getMessage());
			return Status.FAILED;
		}
	}

	public Status consumeBatch(List<AvroFlumeEvent> events) {
		try {
			this.consumer.consumeEvents(convertFlume2Event(events));
			return Status.OK;
		} catch (Exception e) {
			LOG.warn("Error consuming batch of events: " + e.getMessage());
			return Status.FAILED;
		}
	}

	@Override
	public final Status append(AvroFlumeEvent event) {
		LOG.debug("Received event: " + event);
		return consumeSingle(event);
	}

	@Override
	public final Status appendBatch(List<AvroFlumeEvent> events) {
		LOG.debug("Received batch: " + events);
		return consumeBatch(events);
	}

	static protected Event convertFlume2Event(AvroFlumeEvent flumeEvent) {
		EventBuilder builder = new EventBuilder();
		builder.setBody(flumeEvent.getBody().array());
		for (CharSequence header : flumeEvent.getHeaders().keySet()) {
			builder.setHeader(header.toString(), flumeEvent.getHeaders().get(header).toString());
		}
		return builder.create();
	}

	static protected List<Event> convertFlume2Event(List<AvroFlumeEvent> flumeEvents) {
		List<Event>	events = new ArrayList<Event>();
		for (AvroFlumeEvent flumeEvent : flumeEvents) {
			events.add(convertFlume2Event(flumeEvent));
		}
		return events;
	}
}

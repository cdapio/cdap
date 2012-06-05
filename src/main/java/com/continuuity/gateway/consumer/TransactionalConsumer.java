package com.continuuity.gateway.consumer;

import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.type.WriteOperation;
import com.continuuity.flow.flowlet.api.Event;
import com.continuuity.flow.flowlet.impl.EventSerializer;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.Consumer;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TransactionalConsumer extends Consumer {

  /**
   * This is the operations executor that we will use to talk to the data-fabric
   */
  @Inject
  private OperationExecutor executor;

	/** This is our Logger class */
	private static final Logger LOG = LoggerFactory
			.getLogger(TransactionalConsumer.class);

	/**
	 * Use this if you don't use Guice to create the consumer
	 * @param executor The operations executor to use
	 */
	public void setExecutor(OperationExecutor executor) {
		this.executor = executor;
	}

	@Override
	protected void single(Event event) throws Exception {
		List<Event> events = new LinkedList<Event>();
		events.add(event);
		this.batch(events);
	}

	@Override
	protected void batch(List<Event> events) throws Exception {
		List<WriteOperation> operations = new ArrayList<WriteOperation>(events.size());
		EventSerializer serializer = new EventSerializer();
		for (Event event : events) {
			byte[] bytes = serializer.serialize(event);
			if (bytes == null) {
				LOG.warn("Could not serialize event: " + event);
				throw new Exception("Could not serialize event: " + event);
			}
			String destination = event.getHeader(Constants.HEADER_DESTINATION_STREAM);
			if (destination == null) {
				LOG.warn("Enqueuing an event that has no destination. Using queue 'default' instead.");
				destination = "default";
			}
			operations.add(new QueueEnqueue(destination.getBytes(), bytes));
		}
		BatchOperationResult result = this.executor.execute(operations);
		if (!result.isSuccess()) {
			Exception e = new Exception("Failed to enqueue event(s). " + result.getMessage());
			LOG.error(e.getMessage(), e);
			throw e;
		}
	}

}

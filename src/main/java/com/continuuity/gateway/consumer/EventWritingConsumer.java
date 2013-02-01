package com.continuuity.gateway.consumer;

import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.EventSerializer;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.Consumer;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EventWritingConsumer extends Consumer {

  /**
   * This is the operations executor that we will use to talk to the data-fabric
   */
  @Inject
  private OperationExecutor executor;

  /**
   * To avoid the overhead of creating new serializer for every event,
   * we keep a serializer for each thread in a thread local structure.
   */
  ThreadLocal<EventSerializer> serializers =
      new ThreadLocal<EventSerializer>();

  /**
   * Utility method to get or create the thread local serializer
   */
  EventSerializer getSerializer() {
    if (this.serializers.get() == null) {
      this.serializers.set(new EventSerializer());
    }
    return this.serializers.get();
  }

  /**
   * This is our Logger class
   */
  private static final Logger LOG = LoggerFactory
      .getLogger(EventWritingConsumer.class);

  /**
   * Use this if you don't use Guice to create the consumer
   *
   * @param executor The operations executor to use
   */
  public void setExecutor(OperationExecutor executor) {
    this.executor = executor;
  }

  private QueueEnqueue constructOperation(Event event) throws Exception {
    EventSerializer serializer = getSerializer();
    byte[] bytes = serializer.serialize(event);
    if (bytes == null) {
      LOG.warn("Could not serialize event: " + event);
      throw new Exception("Could not serialize event: " + event);
    }
    String destination = event.getHeader(Constants.HEADER_DESTINATION_STREAM);
    if (destination == null) {
      LOG.warn("Enqueuing an event that has no destination. " +
          "Using 'default' instead.");
      destination = "default";
    }
    // construct the stream URO to use for the data fabric
    String queueURI = FlowStream.
        buildStreamURI(Constants.defaultAccount, destination).toString();
    LOG.trace("Sending event to " + queueURI + ", event = " + event);

    return new QueueEnqueue(queueURI.getBytes(), bytes);
  }


  @Override
  protected void single(Event event) throws Exception {
    try {
      QueueEnqueue enqueue = constructOperation(event);
      this.executor.execute(OperationContext.DEFAULT, enqueue);
    } catch (Exception e) {
      Exception e1 = new Exception(
          "Failed to enqueue event(s): " + e.getMessage(), e);
      LOG.error(e.getMessage(), e);
      throw e1;
    }
  }

  @Override
  protected void batch(List<Event> events) throws Exception {
    List<WriteOperation> operations = new ArrayList<WriteOperation>(events.size());
    for (Event event : events) {
      operations.add(constructOperation(event));
    }
    try {
      this.executor.execute(OperationContext.DEFAULT, operations);
    } catch (Exception e) {
      Exception e1 = new Exception(
          "Failed to enqueue event(s): " + e.getMessage(), e);
      LOG.error(e.getMessage(), e);
      throw e1;
    }
  }

}

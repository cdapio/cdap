package com.continuuity.gateway.consumer;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntryImpl;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.Consumer;
import com.continuuity.streamevent.StreamEventCodec;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StreamEventWritingConsumer extends Consumer {

  /**
   * This is the operations executor that we will use to talk to the data-fabric
   */
  @Inject
  private OperationExecutor executor;

  /**
   * The codec to serialize events into byte arrays that can we written to the stream
   */
  private final StreamEventCodec serializer = new StreamEventCodec();

  /**
   * Utility method to get or create the thread local serializer
   */
  StreamEventCodec getSerializer() {
    return this.serializer;
  }

  /**
   * This is our Logger class
   */
  private static final Logger LOG = LoggerFactory
      .getLogger(StreamEventWritingConsumer.class);

  /**
   * Use this if you don't use Guice to create the consumer
   *
   * @param executor The operations executor to use
   */
  public void setExecutor(OperationExecutor executor) {
    this.executor = executor;
  }

  private QueueEnqueue constructOperation(StreamEvent event) throws Exception {
    StreamEventCodec serializer = getSerializer();
    byte[] bytes = serializer.encodePayload(event);
    if (bytes == null) {
      LOG.warn("Could not serialize event: " + event);
      throw new Exception("Could not serialize event: " + event);
    }
    String destination = event.getHeaders().get(Constants.HEADER_DESTINATION_STREAM);
    if (destination == null) {
      LOG.warn("Enqueuing an event that has no destination. " +
          "Using 'default' instead.");
      destination = "default";
    }
    // construct the stream URO to use for the data fabric
    String queueURI = FlowStream.
        buildStreamURI(Constants.defaultAccount, destination).toString();
    LOG.trace("Sending event to " + queueURI + ", event = " + event);

    return new QueueEnqueue(queueURI.getBytes(), new QueueEntryImpl(bytes));
  }


  @Override
  protected void single(StreamEvent event) throws Exception {
    try {
      QueueEnqueue enqueue = constructOperation(event);
      this.executor.commit(OperationContext.DEFAULT, enqueue);
    } catch (Exception e) {
      Exception e1 = new Exception(
          "Failed to enqueue event(s): " + e.getMessage(), e);
      LOG.error(e.getMessage(), e);
      throw e1;
    }
  }

  @Override
  protected void batch(List<StreamEvent> events) throws Exception {
    List<WriteOperation> operations = new ArrayList<WriteOperation>(events.size());
    for (StreamEvent event : events) {
      operations.add(constructOperation(event));
    }
    try {
      this.executor.commit(OperationContext.DEFAULT, operations);
    } catch (Exception e) {
      Exception e1 = new Exception(
          "Failed to enqueue event(s): " + e.getMessage(), e);
      LOG.error(e.getMessage(), e);
      throw e1;
    }
  }

}

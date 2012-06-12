package com.continuuity.gateway.consumer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.data.WriteOperation;
import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.core.TupleSerializer;
import com.continuuity.flow.flowlet.impl.TupleBuilderImpl;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.Consumer;
import com.google.inject.Inject;

public class TupleWritingConsumer extends Consumer {

  /**
   * This is the operations executor that we will use to talk to the data-fabric
   */
  @Inject
  private OperationExecutor executor;

  /**
   * This is our Logger class
   */
  private static final Logger LOG = LoggerFactory
      .getLogger(TupleWritingConsumer.class);

  /**
   * Use this if you don't use Guice to create the consumer
   *
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
    TupleSerializer serializer = new TupleSerializer(false);
    for (Event event : events) {
      Tuple tuple = new TupleBuilderImpl().
          set("headers", event.getHeaders()).
          set("body", event.getBody()).
          create();
      byte[] bytes = serializer.serialize(tuple);
      if (bytes == null) {
        Exception e = new Exception("Could not serialize event: " + event);
        LOG.warn("Could not serialize event: " + event, e);
        throw e;
      }
      String destination = event.getHeader(Constants.HEADER_DESTINATION_STREAM);
      if (destination == null) {
        LOG.debug("Enqueuing an event that has no destination. Using 'default' instead.");
        destination = "default";
      }
      // construct the stream URO to use for the data fabric
      String queueURI = FlowStream.buildStreamURI(destination);
      operations.add(new QueueEnqueue(queueURI.getBytes(), bytes));
    }
    BatchOperationResult result = this.executor.execute(operations);
    if (!result.isSuccess()) {
      Exception e = new Exception("Failed to enqueue event(s). " + result.getMessage());
      LOG.error(e.getMessage(), e);
      throw e;
    }
  }

}

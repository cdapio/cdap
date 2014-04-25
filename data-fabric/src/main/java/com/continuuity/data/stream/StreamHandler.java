/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamEventData;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.stream.DefaultStreamEventData;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST call to stream endpoints.
 *
 * For writing to stream, it uses a non-blocking algorithm to batch writes from concurrent threads.
 * The algorithm is like this:
 *
 * When a thread that received a request, for each stream, performs the following:
 *
 * 1. Constructs a StreamEventData locally and enqueue it to a ConcurrentLinkedQueue.
 * 2. Use CAS to set an AtomicBoolean flag to true.
 * 3. If successfully set the flag to true, this thread becomes the writer and proceed to run step 4-7.
 * 4. Keep polling StreamEventData from the concurrent queue and write to FileWriter with the current timestamp until
 *    the queue is empty.
 * 5. Perform a writer flush to make sure all data written are persisted.
 * 6. Set the state of each StreamEventData that are written to COMPLETED (succeed/failure).
 * 7. Set the AtomicBoolean flag back to false.
 * 8. If the StreamEventData enqueued by this thread is NOT COMPLETED, go back to step 2.
 *
 * The spin lock between step 2 to step 8 is necessary as it guarantees events enqueued by all threads would eventually
 * get written and flushed.
 *
 * TODO: Currently stream "dataset" is implementing old dataset API, hence not supporting multi-tenancy.
 */
@Path(Constants.Gateway.GATEWAY_VERSION + "/streams")
public final class StreamHandler extends AuthenticatedHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);

  private final StreamAdmin streamAdmin;
  private final StreamFileWriterFactory writerFactory;
  private final ConcurrentMap<String, EventQueue> eventQueues;

  @Inject
  public StreamHandler(Authenticator authenticator, StreamAdmin streamAdmin,
                       StreamFileWriterFactory writerFactory, CConfiguration cConf) {
    super(authenticator);
    this.streamAdmin = streamAdmin;
    this.writerFactory = writerFactory;

    int workerThreads = cConf.getInt(Constants.Stream.WORKER_THREADS, 10);
    this.eventQueues = new MapMaker().concurrencyLevel(workerThreads).makeMap();
  }

  @Override
  public void destroy(HandlerContext context) {
    for (EventQueue queue : eventQueues.values()) {
      try {
        queue.close();
      } catch (IOException e) {
        LOG.warn("Failed to close writer.", e);
      }
    }
  }

  @GET
  @Path("/{stream}/info")
  public void info(HttpRequest request, HttpResponder responder,
                   @PathParam("stream") String stream) throws Exception {
    // Just call to verify, but not using the accountId returned.
    getAuthenticatedAccountId(request);

    if (streamAdmin.exists(stream)) {
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @PUT
  @Path("/{stream}")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("stream") String stream) throws Exception {

    getAuthenticatedAccountId(request);

    // TODO: Modify the REST API to support custom configurations.
    streamAdmin.create(stream);

    // TODO: For create successful, 201 Created should be returned instead of 200.
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}")
  public void enqueue(HttpRequest request, HttpResponder responder,
                      @PathParam("stream") String stream) throws Exception {

    getAuthenticatedAccountId(request);

    EventQueue eventQueue = getEventQueue(stream);
    HandlerStreamEventData event = eventQueue.add(getHeaders(request, stream), request.getContent().toByteBuffer());
    do {
      if (!eventQueue.tryWrite()) {
        Thread.yield();
      }
    } while (!event.isCompleted());

    if (event.isSuccess()) {
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/{stream}/dequeue")
  public void dequeue(HttpRequest request, HttpResponder responder, @PathParam("stream") String stream) {
    getAuthenticatedAccountId(request);

    // TODO
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}/consumer")
  public void newConsumer(HttpRequest request, HttpResponder responder, @PathParam("stream") String stream) {
    getAuthenticatedAccountId(request);
    // TODO
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/{stream}/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                       @PathParam("stream") String stream) throws Exception {
    getAuthenticatedAccountId(request);

    // TODO: This is not thread and multi-instances safe yet
    // Need to communicates with other instances with a barrier for closing current file for the given stream
    getEventQueue(stream).close();
    eventQueues.remove(stream);

    if (streamAdmin.exists(stream)) {
      streamAdmin.truncate(stream);
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  private EventQueue getEventQueue(String streamName) throws IOException {
    EventQueue eventQueue = eventQueues.get(streamName);
    if (eventQueue != null) {
      return eventQueue;
    }
    eventQueue = new EventQueue(streamName, createWriterSupplier(streamName));
    EventQueue oldQueue = eventQueues.putIfAbsent(streamName, eventQueue);
    return (oldQueue == null) ? eventQueue : oldQueue;
  }

  private Supplier<FileWriter<StreamEvent>> createWriterSupplier(final String streamName) {
    return new Supplier<FileWriter<StreamEvent>>() {
      @Override
      public FileWriter<StreamEvent> get() {
        try {
          return writerFactory.create(streamName);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private Map<String, String> getHeaders(HttpRequest request, String stream) {
    // build a new event from the request, start with the headers
    ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
    // set some built-in headers
    headers.put(Constants.Gateway.HEADER_FROM_COLLECTOR, Constants.Gateway.STREAM_HANDLER_NAME);
    headers.put(Constants.Gateway.HEADER_DESTINATION_STREAM, stream);
    // and transfer all other headers that are to be preserved
    String prefix = stream + ".";
    for (Map.Entry<String, String> header : request.getHeaders()) {
      if (header.getKey().startsWith(prefix)) {
        headers.put(header);
      }
    }
    return headers.build();
  }

  /**
   * For buffering StreamEvents and doing batch write to stream file.
   */
  @ThreadSafe
  private final class EventQueue implements Closeable {

    private final String streamName;
    private final Supplier<FileWriter<StreamEvent>> writerSupplier;
    private final Queue<HandlerStreamEventData> queue;
    private final AtomicBoolean writerFlag;
    private final SettableStreamEvent streamEvent;

    EventQueue(String streamName, Supplier<FileWriter<StreamEvent>> writerSupplier) {
      this.streamName = streamName;
      this.writerSupplier = Suppliers.memoize(writerSupplier);
      this.queue = new ConcurrentLinkedQueue<HandlerStreamEventData>();
      this.writerFlag = new AtomicBoolean(false);
      this.streamEvent = new SettableStreamEvent();
    }

    HandlerStreamEventData add(Map<String, String> headers, ByteBuffer body) {
      HandlerStreamEventData eventData = new HandlerStreamEventData(headers, body);
      queue.add(eventData);
      return eventData;
    }

    /**
     * Attempts to write the queued events into the underlying stream.
     *
     * @return true if become the writer leader and performed the write, false otherwise.
     */
    boolean tryWrite() {
      if (!writerFlag.compareAndSet(false, true)) {
        return false;
      }

      // The visibility of states mutation done while getting hold of the writerFlag,
      // is piggy back on the writerFlag atomic variable update in the finally block,
      // hence all states mutated will be visible to all threads after that.
      Queue<HandlerStreamEventData> processQueue = Lists.newLinkedList();
      try {
        FileWriter<StreamEvent> writer = writerSupplier.get();
        HandlerStreamEventData data = queue.poll();
        long timestamp = System.currentTimeMillis();
        while (data != null) {
          processQueue.add(data);
          writer.append(streamEvent.set(data, timestamp));
          data = queue.poll();
        }
        writer.flush();
        for (HandlerStreamEventData processed : processQueue) {
          processed.setState(HandlerStreamEventData.State.SUCCESS);
        }
      } catch (Throwable t) {
        LOG.error("Failed to write to file for stream {}.", streamName, t);
        // On exception, remove this EventQueue from the map and close the writer associated with this instance
        eventQueues.remove(streamName, this);
        Closeables.closeQuietly(writerSupplier.get());

        for (HandlerStreamEventData processed : processQueue) {
          processed.setState(HandlerStreamEventData.State.FAILURE);
        }
      } finally {
        writerFlag.set(false);
      }

      return true;
    }

    @Override
    public void close() throws IOException {
      boolean done = false;
      while (!done) {
        if (!writerFlag.compareAndSet(false, true)) {
          Thread.yield();
          continue;
        }
        try {
          // Close is only called from the handler destroy method, hence no need to worry about pending events
          // in the queue, as the http service already closed the connection, hence no guarantee on whether
          // the event is persisted or not.
          writerSupplier.get().close();
        } finally {
          done = true;
          writerFlag.set(false);
        }
      }
    }
  }

  /**
   * A {@link StreamEventData} that carry state on whether it's been written to the underlying stream file or not.
   */
  private static final class HandlerStreamEventData extends DefaultStreamEventData {

    /**
     * The possible state of the event data.
     */
    enum State {
      PENDING,
      SUCCESS,
      FAILURE
    }

    private State state;

    public HandlerStreamEventData(Map<String, String> headers, ByteBuffer body) {
      super(headers, body);
      this.state = State.PENDING;
    }

    public boolean isCompleted() {
      return state != State.PENDING;
    }

    public boolean isSuccess() {
      return state == State.SUCCESS;
    }

    public void setState(State state) {
      this.state = state;
    }
  }

  /**
   * A mutable {@link StreamEvent} that allows setting the data and timestamp. Used by the writer thread
   * to save object creation. It doesn't need to be thread safe as there would be used by the active writer thread
   * only.
   *
   * @see StreamHandler
   */
  private static final class SettableStreamEvent implements StreamEvent {

    private StreamEventData data;
    private long timestamp;

    /**
     * Sets the event data and timestamp.

     * @return this instance.
     */
    public StreamEvent set(StreamEventData data, long timestamp) {
      this.data = data;
      this.timestamp = timestamp;
      return this;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public ByteBuffer getBody() {
      return data.getBody();
    }

    @Override
    public Map<String, String> getHeaders() {
      return data.getHeaders();
    }
  }
}

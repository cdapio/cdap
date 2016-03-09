/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Implementation of {@link HttpServiceResponder} which delegates calls to
 * the HttpServiceResponder's methods to the matching methods for a {@link HttpResponder}.
 * A response is buffered until execute() is called. This allows you to send the correct response upon
 * a transaction failure, and to not always delegating to the user response.
 */
public class DelayedHttpServiceResponder extends AbstractHttpServiceResponder implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(DelayedHttpServiceResponder.class);

  private final HttpResponder responder;
  private final BodyProducerFactory bodyProducerFactory;
  private final TransactionalHttpServiceContext serviceContext;
  private final MetricsContext metricsContext;
  private BufferedResponse bufferedResponse;
  private boolean closed;

  /**
   * Instantiates the class from a {@link HttpResponder}
   *
   * @param responder the responder which will be bound to
   */
  public DelayedHttpServiceResponder(HttpResponder responder, BodyProducerFactory bodyProducerFactory,
                                     TransactionalHttpServiceContext serviceContext, MetricsContext metricsContext) {
    this.responder = responder;
    this.serviceContext = serviceContext;
    this.metricsContext = metricsContext;
    this.bodyProducerFactory = bodyProducerFactory;
  }

  /**
   * Intantiates the class from another {@link DelayedHttpServiceResponder}
   * with a different {@link BodyProducerFactory}.
   */
  DelayedHttpServiceResponder(DelayedHttpServiceResponder other, BodyProducerFactory bodyProducerFactory) {
    this.responder = other.responder;
    this.bodyProducerFactory = bodyProducerFactory;
    this.serviceContext = other.serviceContext;
    this.metricsContext = other.metricsContext;
    this.bufferedResponse = other.bufferedResponse;
  }

  @Override
  protected void doSend(int status, String contentType,
                        @Nullable ChannelBuffer content,
                        @Nullable HttpContentProducer contentProducer,
                        @Nullable Multimap<String, String> headers) {
    Preconditions.checkState(!closed,
     "Responder is already closed. " +
       "This may due to either using a HttpServiceResponder inside HttpContentProducer or " +
       "not using HttpServiceResponder provided to the HttpContentConsumer onFinish/onError method.");

    if (bufferedResponse != null) {
      LOG.warn("Multiple calls to one of the 'send*' methods has been made. Only the last response will be sent.");
    }
    bufferedResponse = new BufferedResponse(status, contentType, content, contentProducer, headers);
  }

  /**
   * Returns {@code true} if there is a buffered response. This means any of the send methods was called.
   */
  public boolean hasBufferedResponse() {
    return bufferedResponse != null;
  }

  /**
   * Returns {@code true} if a {@link HttpContentProducer} will be used to produce response body.
   */
  public boolean hasContentProducer() {
    return hasBufferedResponse() && bufferedResponse.getContentProducer() != null;
  }

  /**
   * Since calling one of the send methods multiple times logs a warning, upon transaction failures this
   * method is called to allow setting the failure response without an additional warning.
   */
  public void setTransactionFailureResponse(Throwable t) {
    LOG.error("Exception occurred while handling request:", t);
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    ChannelBuffer content = ChannelBuffers.copiedBuffer("Exception occurred while handling request: "
                                                          + Throwables.getRootCause(t).getMessage(), Charsets.UTF_8);

    bufferedResponse = new BufferedResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(),
                                            "text/plain; charset=" + Charsets.UTF_8.name(),
                                            content, null, null);
  }

  /**
   * Same as calling {@link #execute(boolean) execute(true)}.
   */
  public void execute() {
    execute(true);
  }

  /**
   * Calls to other responder methods in this class only cache the response to be sent. The response is actually
   * sent only when this method is called.
   *
   * @param keepAlive {@code true} to keep the connection open; {@code false} otherwise
   */
  public void execute(boolean keepAlive) {
    Preconditions.checkState(bufferedResponse != null,
                             "Can not call execute before one of the other responder methods are called.");

    try {
      HttpContentProducer contentProducer = bufferedResponse.getContentProducer();

      if (contentProducer == null) {
        // If content producer is not used, we can dismiss the transaction context since all
        // transactional operations are completed at this point.
        // If content producer is used, the user provided content producer might have closure over datasets so that
        // we cannot dismiss the transaction context here. The dismissal will be done on the completion of
        // the content producer.
        serviceContext.dismissTransactionContext();
      }

      Multimap<String, String> headers = LinkedListMultimap.create(bufferedResponse.getHeaders());
      headers.put(HttpHeaders.Names.CONNECTION, keepAlive ? HttpHeaders.Values.KEEP_ALIVE : HttpHeaders.Values.CLOSE);
      if (!headers.containsKey(HttpHeaders.Names.CONTENT_TYPE)) {
        headers.put(HttpHeaders.Names.CONTENT_TYPE, bufferedResponse.getContentType());
      }

      if (contentProducer != null) {
        responder.sendContent(HttpResponseStatus.valueOf(bufferedResponse.getStatus()),
                              bodyProducerFactory.create(contentProducer, serviceContext),
                              headers);
      } else {
        responder.sendContent(HttpResponseStatus.valueOf(bufferedResponse.getStatus()),
                              bufferedResponse.getContentBuffer(),
                              bufferedResponse.getContentType(), headers);
      }
      emitMetrics(bufferedResponse.getStatus());
    } finally {
      close();
    }
  }

  private void emitMetrics(int status) {
    StringBuilder builder = new StringBuilder(50);
    builder.append("response.");
    if (status < 100) {
      builder.append("unknown");
    } else if (status < 200) {
      builder.append("information");
    } else if (status < 300) {
      builder.append("successful");
    } else if (status < 400) {
      builder.append("redirect");
    } else if (status < 500) {
      builder.append("client.error");
    } else if (status < 600) {
      builder.append("server.error");
    } else {
      builder.append("unknown");
    }
    builder.append(".count");

    metricsContext.increment(builder.toString(), 1);
    metricsContext.increment("requests.count", 1);
  }

  @Override
  public void close() {
    closed = true;
  }

  private static final class BufferedResponse {

    private final int status;
    private final ChannelBuffer contentBuffer;
    private final HttpContentProducer contentProducer;
    private final String contentType;
    private final Multimap<String, String> headers;

    private BufferedResponse(int status, String contentType,
                             @Nullable ChannelBuffer contentBuffer,
                             @Nullable HttpContentProducer contentProducer,
                             @Nullable Multimap<String, String> headers) {
      this.status = status;
      this.contentType = contentType;
      this.contentBuffer = contentBuffer;
      this.contentProducer = contentProducer;
      this.headers = headers == null ? ImmutableMultimap.<String, String>of() : ImmutableMultimap.copyOf(headers);
    }

    public int getStatus() {
      return status;
    }

    @Nullable
    public ChannelBuffer getContentBuffer() {
      return contentBuffer;
    }

    @Nullable
    public HttpContentProducer getContentProducer() {
      return contentProducer;
    }

    public String getContentType() {
      return contentType;
    }

    public Multimap<String, String> getHeaders() {
      return headers;
    }
  }
}

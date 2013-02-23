package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 *
 */
final class HttpProcedureResponder implements ProcedureResponder {

  private final Channel channel;
  private ProcedureResponse.Writer writer;

  HttpProcedureResponder(Channel channel) {
    this.channel = channel;
  }

  @Override
  public synchronized ProcedureResponse.Writer stream(ProcedureResponse response) throws IOException {
    if (writer != null) {
      return writer;
    }

    HttpResponse httpResponse = createHttpResponse(response);
    if (!httpResponse.getStatus().equals(HttpResponseStatus.OK)) {
      handleFailure(httpResponse);
      return writer;
    }

    httpResponse.setChunked(true);
    httpResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/octet-stream");
    channel.write(httpResponse);
    writer = new HttpResponseWriter(channel);

    return writer;
  }

  @Override
  public synchronized void sendJson(ProcedureResponse response, Object object) throws IOException {
    if (writer != null) {
      throw new IOException("A writer is already opened for streaming or the response was already sent.");
    }

    try {
      HttpResponse httpResponse = createHttpResponse(response);
      if (!httpResponse.getStatus().equals(HttpResponseStatus.OK)) {
        handleFailure(httpResponse);
        return;
      }

      ChannelBuffer channelBuffer = ChannelBuffers.dynamicBuffer();
      JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(new ChannelBufferOutputStream(channelBuffer),
                                                                    Charsets.UTF_8));
      new Gson().toJson(object, object.getClass(), jsonWriter);
      jsonWriter.close();

      httpResponse.setContent(channelBuffer);
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json");
      ChannelFuture result = channel.write(channelBuffer);
      result.addListener(ChannelFutureListener.CLOSE);
      result.await();
    } catch (Throwable t) {
      throw new IOException(t);
    } finally {
      writer = ResponseWriters.CLOSED_WRITER;
    }
  }

  private void handleFailure(HttpResponse httpResponse) {
    httpResponse.setContent(ChannelBuffers.EMPTY_BUFFER);
    channel.write(httpResponse).addListener(ChannelFutureListener.CLOSE);
    writer = new ClosedResponseWriter();
  }

  private HttpResponse createHttpResponse(ProcedureResponse response) throws IOException {
    Preconditions.checkNotNull(response, "ProcedureResponse cannot be null.");
    HttpResponseStatus status;
    switch (response.getCode()) {
      case SUCCESS:
        status = HttpResponseStatus.OK;
        break;
      case FAILURE:
        status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        break;
      case CLIENT_ERROR:
        status = HttpResponseStatus.BAD_REQUEST;
        break;
      default:
        status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }

    return new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
  }
}

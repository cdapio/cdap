/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.procedure;

import co.cask.cdap.api.procedure.ProcedureResponse;
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
final class HttpProcedureResponder extends AbstractProcedureResponder {

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

    httpResponse.setHeader(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
    httpResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/octet-stream");
    try {
      channel.write(httpResponse).await();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
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
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, channelBuffer.readableBytes());
      ChannelFuture result = channel.write(httpResponse);
      result.addListener(ChannelFutureListener.CLOSE);
      result.await();
    } catch (Throwable t) {
      throw new IOException(t);
    } finally {
      writer = ResponseWriters.CLOSED_WRITER;
    }
  }

  @Override
  public synchronized void error(ProcedureResponse.Code errorCode, String errorMessage) throws IOException {
    Preconditions.checkArgument(errorCode != ProcedureResponse.Code.SUCCESS, "Cannot send SUCCESS as error.");

    if (writer != null) {
      throw new IOException("A writer is already opened for streaming or the response was already sent.");
    }

    try {
      ChannelBuffer errorContent = ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(errorMessage));
      HttpResponse httpResponse = createHttpResponse(new ProcedureResponse(errorCode));
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, errorContent.readableBytes());
      httpResponse.setContent(errorContent);
      channel.write(httpResponse).addListener(ChannelFutureListener.CLOSE);
    } finally {
      writer = ResponseWriters.CLOSED_WRITER;
    }
  }

  private void handleFailure(HttpResponse httpResponse) {
    httpResponse.setContent(ChannelBuffers.EMPTY_BUFFER);
    channel.write(httpResponse).addListener(ChannelFutureListener.CLOSE);
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
      case NOT_FOUND:
        status = HttpResponseStatus.NOT_FOUND;
        break;
      default:
        status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }

    return new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
  }
}

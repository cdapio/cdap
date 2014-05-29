package com.continuuity.common.exception;

import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

/**
 * Exception handling for failures in netty pipeline.
 */
public final class HandlerException extends RuntimeException {

  private final HttpResponseStatus failureStatus;
  private String message;

  public HandlerException(HttpResponseStatus failureStatus, String message) {
    super(message);
    this.failureStatus = failureStatus;
    this.message = message;
  }

  public HandlerException(HttpResponseStatus failureStatus, String message, Throwable cause) {
    super(message, cause);
    this.failureStatus = failureStatus;
    this.message = message;
  }

  public HttpResponse createFailureResponse() {
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, failureStatus);
    response.setContent(ChannelBuffers.copiedBuffer(message, Charsets.UTF_8));
    return response;
  }

  public HttpResponseStatus getFailureStatus() {
    return failureStatus;
  }

  public String getMessage() {
    return message;
  }
}

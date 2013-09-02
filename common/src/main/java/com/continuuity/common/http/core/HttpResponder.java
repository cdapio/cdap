/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelFutureProgressListener;
import org.jboss.netty.channel.DefaultFileRegion;
import org.jboss.netty.channel.FileRegion;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedFile;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CACHE_CONTROL;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.DATE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.EXPIRES;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.LAST_MODIFIED;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.setContentLength;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * HttpResponder responds back to the client that initiated the request. Caller can use sendJson method to respond
 * back to the client in json format.
 */
public class HttpResponder {
  private final Channel channel;
  private final boolean keepalive;
  public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
  public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
  public static final int HTTP_CACHE_SECONDS = 60;
  public static final MimetypesFileTypeMap mimeTypesMap;

  static {
    mimeTypesMap = new MimetypesFileTypeMap();
    mimeTypesMap.addMimeTypes("image/png png PNG");
    mimeTypesMap.addMimeTypes("image/gif gif GIF");
    mimeTypesMap.addMimeTypes("image/jpeg jpeg jpg JPEG JPG");
    mimeTypesMap.addMimeTypes("text  txt text rtf TXT");
    mimeTypesMap.addMimeTypes("application/javascript js JS");
    mimeTypesMap.addMimeTypes("text/css css CSS");
    mimeTypesMap.addMimeTypes("application/x-font-woff woff WOFF");
  }


  public HttpResponder(Channel channel, boolean keepalive) {
    this.channel = channel;
    this.keepalive = keepalive;
  }

  /**
   * Sends json response back to the client.
   * @param status Status of the response.
   * @param object Object that will be serialized into Json and sent back as content.
   */
  public void sendJson(HttpResponseStatus status, Object object){
    try {
      ChannelBuffer channelBuffer = ChannelBuffers.dynamicBuffer();
      JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(new ChannelBufferOutputStream(channelBuffer),
                                                                    Charsets.UTF_8));
      new Gson().toJson(object, object.getClass(), jsonWriter);
      jsonWriter.close();

      sendContent(status, channelBuffer, "application/json", ImmutableMultimap.<String, String>of());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Send a string response back to the http client.
   * @param status status of the Http response.
   * @param data string data to be sent back.
   */
  public void sendString(HttpResponseStatus status, String data){
    try {
      ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(data));
      sendContent(status, channelBuffer, "text/plain; charset=utf-8", ImmutableMultimap.<String, String>of());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Send only a status code back to client without any content.
   * @param status status of the Http response.
   */
  public void sendStatus(HttpResponseStatus status) {
    sendContent(status, null, null, ImmutableMultimap.<String, String>of());
  }

  /**
   * Send a response containing raw bytes. Sets "application/octet-stream" as content type header.
   * @param status status of the Http response.
   * @param bytes bytes to be sent back.
   * @param headers headers to be sent back. This will overwrite any headers set by the framework.
   */
  public void sendByteArray(HttpResponseStatus status, byte [] bytes, Multimap<String, String> headers) {
    ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(bytes);
    sendContent(status, channelBuffer, "application/octet-stream", headers);
  }

  /**
   * Sends error message back to the client.
   *
   * @param status Status of the response.
   * @param errorMessage Error message sent back to the client.
   */
  public void sendError(HttpResponseStatus status, String errorMessage){
    Preconditions.checkArgument(!status.equals(HttpResponseStatus.OK), "Response status cannot be OK for errors");

    ChannelBuffer errorContent = ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(errorMessage));
    sendContent(status, errorContent, "text/plain; charset=utf-8", ImmutableMultimap.<String, String>of());
  }

  private void sendContent(HttpResponseStatus status, ChannelBuffer content, String contentType,
                           Multimap<String, String> headers){
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

    if (content != null) {
      response.setContent(content);
      response.setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType);
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());
    } else {
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
    }

    if (keepalive) {
      response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    }

    // Add headers, note will override all headers set by the framework
    for (Map.Entry<String, Collection<String>> entry : headers.asMap().entrySet()) {
      response.setHeader(entry.getKey(), entry.getValue());
    }

    ChannelFuture future = channel.write(response);
    if (!keepalive) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  public void sendNotModifiedHeader() {
    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, NOT_MODIFIED);
    SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
    dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

    Calendar time = new GregorianCalendar();
    response.setHeader(DATE, dateFormatter.format(time.getTime()));
    channel.write(response).addListener(ChannelFutureListener.CLOSE);
  }

  public void sendFile(final String path, final File file) throws IOException, ParseException {
    RandomAccessFile raf;
    try {
      raf = new RandomAccessFile(file, "r");
    } catch (FileNotFoundException fnfe) {
      sendStatus(NOT_FOUND);
      return;
    }
    long fileLength = raf.length();

    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
    setContentLength(response, fileLength);
    setContentTypeHeader(response, file);
    setDateAndCacheHeaders(response, file);

    // Write the initial line and the header.
    channel.write(response);

    // Write the content.
    ChannelFuture writeFuture;
    if (channel.getPipeline().get(SslHandler.class) != null) {
      // Cannot use zero-copy with HTTPS.
      writeFuture = channel.write(new ChunkedFile(raf, 0, fileLength, 8192));
    } else {
      // No encryption - use zero-copy.
      final FileRegion region =
        new DefaultFileRegion(raf.getChannel(), 0, fileLength);
      writeFuture = channel.write(region);
      writeFuture.addListener(new ChannelFutureProgressListener() {
        public void operationComplete(ChannelFuture future) {
          region.releaseExternalResources();
        }

        public void operationProgressed(
          ChannelFuture future, long amount, long current, long total) {
          System.out.printf("%s: %d / %d (+%d)%n", path, current, total, amount);
        }
      });
    }

    // Close the connection when the whole content is written out.
    writeFuture.addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Sets the Date and Cache headers for the HTTP Response
   *
   * @param response
   *            HTTP response
   * @param fileToCache
   *            file to extract content type
   */
  private static void setDateAndCacheHeaders(HttpResponse response, File fileToCache) {
    SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
    dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

    // Date header
    Calendar time = new GregorianCalendar();
    response.setHeader(DATE, dateFormatter.format(time.getTime()));

    // Add cache headers
    time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
    response.setHeader(EXPIRES, dateFormatter.format(time.getTime()));
    response.setHeader(CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
    response.setHeader(
      LAST_MODIFIED, dateFormatter.format(new Date(fileToCache.lastModified())));
  }

  /**
   * Sets the content type header for the HTTP Response
   *
   * @param response
   *            HTTP response
   * @param file
   *            file to extract content type
   */
  private static void setContentTypeHeader(HttpResponse response, File file) {
    response.setHeader(CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));
  }
}

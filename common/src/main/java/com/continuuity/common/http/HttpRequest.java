/*
 * Copyright (c) 2012-2014 Continuuity Inc. All rights reserved.
 */
package com.continuuity.common.http;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents an HTTP request to be executed by {@link HttpRequests}.
 */
public class HttpRequest {

  private final HttpMethod method;
  private final URL url;
  private final Map<String, String> headers;
  private final InputSupplier<? extends InputStream> body;

  public HttpRequest(HttpMethod method, URL url, @Nullable Map<String, String> headers,
                     @Nullable InputSupplier<? extends InputStream> body) {
    this.method = method;
    this.url = url;
    this.headers = headers;
    this.body = body;
  }

  public HttpRequest(HttpMethod method, URL url) {
    this(method, url, null, (InputSupplier<? extends InputStream>) null);
  }

  public HttpRequest(HttpMethod method, URL url, InputSupplier<? extends InputStream> body) {
    this(method, url, null, body);
  }

  public HttpRequest(HttpMethod method, URL url, Map<String, String> headers, File body) {
    this(method, url, headers, Files.newInputStreamSupplier(body));
  }

  public HttpRequest(HttpMethod method, URL url, File body) {
    this(method, url, null, Files.newInputStreamSupplier(body));
  }

  public HttpRequest(HttpMethod method, URL url, String body) {
    this(method, url, null, stringBody(body));
  }

  public HttpRequest(HttpMethod method, URL url, Map<String, String> headers, String body) {
    this(method, url, headers, stringBody(body));
  }

  public HttpRequest(HttpMethod method, URL url, Map<String, String> headers) {
    this(method, url, headers, (InputSupplier<? extends InputStream>) null);
  }

  public static final InputSupplier<? extends InputStream> stringBody(String body) {
    return ByteStreams.newInputStreamSupplier(body.getBytes(Charsets.UTF_8));
  }

  public static final InputSupplier<? extends InputStream> stringBody(String body, Charset charset) {
    return ByteStreams.newInputStreamSupplier(body.getBytes(charset));
  }

  public HttpMethod getMethod() {
    return method;
  }

  public URL getURL() {
    return url;
  }

  @Nullable
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Nullable
  public InputSupplier<? extends InputStream> getBody() {
    return body;
  }
}

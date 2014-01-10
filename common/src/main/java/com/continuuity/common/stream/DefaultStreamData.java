/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.stream;

import com.continuuity.api.stream.StreamData;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Default implementation of {@link StreamData} that takes headers and body from
 * constructor.
 */
public class DefaultStreamData implements StreamData {

  private final Map<String, String> headers;
  private final ByteBuffer body;

  public DefaultStreamData(Map<String, String> headers, ByteBuffer body) {
    this.headers = ImmutableMap.copyOf(headers);
    this.body = body;
  }

  @Override
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public ByteBuffer getBody() {
    return body;
  }
}

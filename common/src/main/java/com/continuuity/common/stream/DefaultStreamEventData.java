/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.stream;

import com.continuuity.api.stream.StreamEventData;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Default implementation of {@link com.continuuity.api.stream.StreamEventData} that takes headers and body from
 * constructor.
 */
public class DefaultStreamEventData implements StreamEventData {

  private final Map<String, String> headers;
  private final ByteBuffer body;

  public DefaultStreamEventData(Map<String, String> headers, ByteBuffer body) {
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

/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * BasicHandlerContext returns an empty runtime arguments.
 */
public class BasicHandlerContext implements HandlerContext {
  private final HttpResourceHandler httpResourceHandler;

  public BasicHandlerContext(HttpResourceHandler httpResourceHandler) {
    this.httpResourceHandler = httpResourceHandler;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
   return ImmutableMap.of();
  }

  @Override
  public HttpResourceHandler getHttpResourceHandler() {
    return httpResourceHandler;
  }
}

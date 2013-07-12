/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

/**
 * A base implementation of {@link HttpHandler} that provides no-op for both {@link #init(HandlerContext)}
 * and {@link #destroy(HandlerContext)} methods.
 */
public abstract class AbstractHttpHandler implements HttpHandler {

  @Override
  public void init(HandlerContext context) {
    // No-op
  }

  @Override
  public void destroy(HandlerContext context) {
    // No-op
  }
}

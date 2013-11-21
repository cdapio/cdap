package com.continuuity.common.http.core;

import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Re-writes URL of an incoming request before any handlers or their hooks are called.
 */
public interface UrlRewriter {
  /**
   * Implement this to rewrite URL of an incoming request. The re-written URL needs to be updated back in
   * {@code request} using {@link HttpRequest#setUri(String)}.
   * @param request Incoming HTTP request.
   */
  void rewrite(HttpRequest request);
}

package com.continuuity.common.http.core;

import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Re-writes URL of an incoming request before any handlers or their hooks are called.
 * This can be used to map an incoming URL to an URL that a handler understands. The re-writer overwrites the incoming
 * URL with the new value.
 */
public interface UrlRewriter {
  /**
   * Implement this to rewrite URL of an incoming request. The re-written URL needs to be updated back in
   * {@code request} using {@link HttpRequest#setUri(String)}.
   *
   * @param request Incoming HTTP request.
   */
  void rewrite(HttpRequest request);
}

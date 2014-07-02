package com.continuuity.gateway.router;

import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Applies rule that may change {@link HttpRequest} being forwarded
 */
public interface ProxyRule {
  HttpRequest apply(HttpRequest request);
}

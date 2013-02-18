package com.continuuity.gateway.auth;

import org.jboss.netty.handler.codec.http.HttpRequest;

import com.continuuity.api.flow.flowlet.Event;

/**
 * Authenticator used when authentication is disabled.
 */
public class NoAuthenticator implements GatewayAuthenticator {

  @Override
  public boolean authenticateRequest(HttpRequest request) {
    return true;
  }

  @Override
  public boolean authenticateRequest(Event event) {
    return true;
  }

  @Override
  public boolean isAuthenticationRequired() {
    return false;
  }

}

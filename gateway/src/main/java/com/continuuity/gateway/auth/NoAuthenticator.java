package com.continuuity.gateway.auth;

import org.apache.flume.source.avro.AvroFlumeEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Authenticator used when authentication is disabled.
 */
public class NoAuthenticator implements GatewayAuthenticator {

  @Override
  public boolean authenticateRequest(HttpRequest request) {
    return true;
  }

  @Override
  public boolean authenticateRequest(AvroFlumeEvent event) {
    return true;
  }

  @Override
  public String getAccountId(HttpRequest httpRequest) {
    return com.continuuity.common.conf.Constants.DEVELOPER_ACCOUNT_ID;
  }

  @Override
  public String getAccountId(AvroFlumeEvent event) {
    return com.continuuity.common.conf.Constants.DEVELOPER_ACCOUNT_ID;
  }

  @Override
  public boolean isAuthenticationRequired() {
    return false;
  }

}

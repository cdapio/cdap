package com.continuuity.gateway.auth;

import com.continuuity.common.conf.Constants;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Authenticator used when authentication is disabled.
 */
public class NoAuthenticator implements Authenticator {

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
    return Constants.DEVELOPER_ACCOUNT_ID;
  }

  @Override
  public String getAccountId(AvroFlumeEvent event) {
    return Constants.DEVELOPER_ACCOUNT_ID;
  }

  @Override
  public boolean isAuthenticationRequired() {
    return false;
  }

}

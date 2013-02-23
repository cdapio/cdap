package com.continuuity.gateway.auth;

import org.apache.flume.source.avro.AvroFlumeEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.util.Map;

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
    // singlenode and other "private" setups we pass accountId in a token header
    String apiKey = httpRequest.getHeader(CONTINUUITY_API_KEY);
    if (apiKey == null) {
      throw new RuntimeException("event was not authenticated");
    }
    return apiKey;
  }

  @Override
  public String getAccountId(AvroFlumeEvent event) {
    // singlenode and other "private" setups we pass accountId in a token header
    for (Map.Entry<CharSequence,CharSequence> headerEntry :
      event.getHeaders().entrySet()) {
      String headerKey = headerEntry.getKey().toString();
      if (headerKey.equals(CONTINUUITY_API_KEY)) {
        return headerEntry.getValue().toString();
      }
    }
    // Key not found in headers
    throw new RuntimeException("event was not authenticated");
  }

  @Override
  public boolean isAuthenticationRequired() {
    return false;
  }

}

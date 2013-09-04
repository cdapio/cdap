package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract handler that support Passport authetication method.
 */
public abstract class AuthenticatedHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticatedHttpHandler.class);
  private final GatewayAuthenticator authenticator;

  @Inject
  public AuthenticatedHttpHandler(GatewayAuthenticator authenticator) {
    this.authenticator = authenticator;
  }

  protected String getAuthenticatedAccountId(HttpRequest request) throws SecurityException, IllegalArgumentException {
    // if authentication is enabled, verify an authentication token has been
    // passed and then verify the token is valid
    if (!authenticator.authenticateRequest(request)) {
      LOG.trace("Received an unauthorized request");
      throw new SecurityException("UnAuthorized access.");
    }

    String accountId = authenticator.getAccountId(request);
    if (accountId == null || accountId.isEmpty()) {
      LOG.trace("No valid account information found");
      throw new IllegalArgumentException("Not a valid account id found.");
    }
    return accountId;
  }

}

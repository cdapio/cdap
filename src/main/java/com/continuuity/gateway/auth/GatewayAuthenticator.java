package com.continuuity.gateway.auth;

import org.jboss.netty.handler.codec.http.HttpRequest;

import com.continuuity.api.flow.flowlet.Event;


/**
 * Interface that supports the authentication of requests to the Gateway.
 * <p>
 * Underlying implementations can choose how they authenticate.  The two current
 * implementations either use no authentication or require a token that will
 * be checked against the passport service as to whether it grants access to
 * the current cluster this gateway is for.
 */
public interface GatewayAuthenticator {

  public final static String CONTINUUITY_API_KEY = "X-Continuuity-ApiKey";

  /**
   * Checks whether authentication is required or not.  If not, then no token
   * is required on any requests.
   * @return true if authentication (and thus token) are required, false if not
   */
  public boolean isAuthenticationRequired();

  /**
   * Authenticates the specified HTTP request.
   * @param request http request
   * @return true if authentication succeeds, false if not
   */
  public boolean authenticateRequest(HttpRequest httpRequest);

  /**
   * Authenticates the specified Stream Event.
   * @param event stream event to authenticate
   * @return true if authentication succeeds, false if not
   */
  public boolean authenticateRequest(Event event);

}

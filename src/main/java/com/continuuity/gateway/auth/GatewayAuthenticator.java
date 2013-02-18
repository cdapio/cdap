package com.continuuity.gateway.auth;

import org.apache.flume.source.avro.AvroFlumeEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;


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
   * Authenticates the specified HTTP request.
   * @param request http request
   * @return true if authentication succeeds, false if not
   */
  public boolean authenticateRequest(AvroFlumeEvent flumeEvent);

}

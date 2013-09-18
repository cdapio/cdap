package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.service.ServerException;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.weave.discovery.Discoverable;
import com.google.inject.Inject;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
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

  /**
   * generic method to discover a thrift service and start up the
   * thrift transport and protocol layer.
   */
  protected TProtocol getThriftProtocol(String serviceName, EndpointStrategy endpointStrategy) throws ServerException {
    Discoverable endpoint = endpointStrategy.pick();
    if (endpoint == null) {
      String message = String.format("Service '%s' is not registered in discovery service.", serviceName);
      LOG.error(message);
      throw new ServerException(message);
    }
    TTransport transport = new TFramedTransport(
      new TSocket(endpoint.getSocketAddress().getHostName(), endpoint.getSocketAddress().getPort()));
    try {
      transport.open();
    } catch (TTransportException e) {
      String message = String.format("Unable to connect to thrift service %s at %s. Reason: %s",
                                     serviceName, endpoint.getSocketAddress(), e.getMessage());
      LOG.error(message);
      throw new ServerException(message, e);
    }
    // now try to connect the thrift client
    return new TBinaryProtocol(transport);
  }

}

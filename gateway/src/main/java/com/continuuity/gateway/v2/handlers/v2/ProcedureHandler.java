package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

/**
 * Handles procedure calls.
 */
@Path("/v2")
public class ProcedureHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureHandler.class);
  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public ProcedureHandler(GatewayAuthenticator authenticator, DiscoveryServiceClient discoveryServiceClient) {
    super(authenticator);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @POST
  @Path("/apps/{appId}/procedures/{procedureName}/methods/{methodName}")
  public void procedureCall(HttpRequest request, HttpResponder responder,
                            @PathParam("appId") String appId, @PathParam("procedureName") String procedureName,
                            @PathParam("methodName") String methodName) {

    try {
      String accountId = getAuthenticatedAccountId(request);

      // determine the service provider for the given path
      String serviceName = String.format("procedure.%s.%s.%s", accountId, appId, procedureName);
      List<Discoverable> endpoints = Lists.newArrayList(discoveryServiceClient.discover(serviceName));
      if (endpoints.isEmpty()) {
        LOG.trace("No endpoint for service {}", serviceName);
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      // make HTTP call to provider
      Collections.shuffle(endpoints);
      InetSocketAddress endpoint = endpoints.get(0).getSocketAddress();
      String relayUri = Joiner.on('/').appendTo(
        new StringBuilder("http://").append(endpoint.getHostName()).append(":").append(endpoint.getPort()).append("/"),
        "apps", appId, "procedures", procedureName, methodName).toString();

      LOG.trace("Relaying request to " + relayUri);

    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
      return;
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }  catch (Throwable e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }
  }
}

package com.continuuity.gateway.auth;

import java.util.List;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.passport.http.client.PassportClient;

/**
 * Authenticator that uses a PassportClient to verify that requests using a
 * specified token are authorized to use the current cluster.
 */
public class PassportVPCAuthenticator implements GatewayAuthenticator {

  private static final Logger LOG =
      LoggerFactory.getLogger(PassportVPCAuthenticator.class);

  private final String clusterName;

  private final String passportHostname;

  private final PassportClient passportClient;

  public PassportVPCAuthenticator(String clusterName, String passportHostname,
      PassportClient passportClient) {
    this.clusterName = clusterName;
    this.passportHostname = passportHostname;
    this.passportClient = passportClient;
  }

  @Override
  public boolean authenticateRequest(HttpRequest request) {
    String apiKey = request.getHeader(CONTINUUITY_API_KEY);
    if (apiKey == null) return false;
    return authenticate(apiKey);
  }

  @Override
  public boolean authenticateRequest(Event event) {
    String apiKey = event.getHeader(CONTINUUITY_API_KEY);
    if (apiKey == null) return false;
    return authenticate(apiKey);
  }

  /**
   * Authenticates that the specified apiKey can access this cluster.
   * @param apiKey
   * @return true
   */
  private boolean authenticate(String apiKey) {
    try {
      List<String> authorizedClusters =
          this.passportClient.getVPCList(this.passportHostname, apiKey);
      if (authorizedClusters == null || authorizedClusters.isEmpty())
        return false;
      for (String authorizedCluster : authorizedClusters) {
        if (clusterName.equals(authorizedCluster)) return true;
      }
      LOG.debug("Failed to authenticate request using key: " + apiKey);
      return false;
    } catch (Exception e) {
      LOG.error("Exception authorizing with passport service", e);
      return false;
    }
  }

  @Override
  public boolean isAuthenticationRequired() {
    return true;
  }

}

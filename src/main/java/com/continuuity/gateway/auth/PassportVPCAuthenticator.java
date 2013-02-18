package com.continuuity.gateway.auth;

import java.util.List;

import org.jboss.netty.handler.codec.http.HttpRequest;

import com.continuuity.gateway.Constants;
import com.continuuity.passport.http.client.PassportClient;

/**
 * Authenticator that uses a PassportClient to verify that requests using a
 * specified token are authorized to use the current cluster.
 */
public class PassportVPCAuthenticator implements GatewayAuthenticator {

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
    try {
      String apiToken = request.getHeader(CONTINUUITY_API_KEY);
      // TODO: We may want a more complex return type to distinguish a missing
      //       api token from an invalid api token
      if (apiToken == null) return false;
      List<String> authorizedClusters =
          this.passportClient.getVPCList(this.passportHostname, apiToken);
      if (authorizedClusters == null || authorizedClusters.isEmpty())
        return false;
      for (String authorizedCluster : authorizedClusters) {
        if (clusterName.equals(authorizedCluster)) return true;
      }
      return false;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public boolean isAuthenticationRequired() {
    return true;
  }

}

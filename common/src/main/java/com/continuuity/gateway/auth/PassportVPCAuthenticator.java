package com.continuuity.gateway.auth;

import com.continuuity.common.conf.Constants;
import com.continuuity.passport.http.client.PassportClient;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Authenticator that uses a PassportClient to verify that requests using a
 * specified token are authorized to use the current cluster.
 */
public class PassportVPCAuthenticator implements Authenticator {

  private static final Logger LOG =
    LoggerFactory.getLogger(PassportVPCAuthenticator.class);

  private final String clusterName;

  private final PassportClient passportClient;

  public PassportVPCAuthenticator(String clusterName, PassportClient passportClient) {
    this.clusterName = clusterName;
    this.passportClient = passportClient;
  }

  @Override
  public boolean authenticateRequest(HttpRequest request) {
    String apiKey = request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY);
    if (apiKey == null) {
      return false;
    }
    return authenticate(apiKey);
  }

  @Override
  public boolean authenticateRequest(AvroFlumeEvent event) {
    for (Map.Entry<CharSequence, CharSequence> headerEntry :
      event.getHeaders().entrySet()) {
      String headerKey = headerEntry.getKey().toString();
      if (headerKey.equals(Constants.Gateway.CONTINUUITY_API_KEY)) {
        return authenticate(headerEntry.getValue().toString());
      }
    }
    // Key not found in headers
    return false;
  }

  @Override
  public String getAccountId(HttpRequest httpRequest) {
    String apiKey = httpRequest.getHeader(Constants.Gateway.CONTINUUITY_API_KEY);
    if (apiKey == null) {
      throw new RuntimeException("http request was not authenticated");
    }
    return this.passportClient.getAccount(apiKey).getAccountId();
  }

  @Override
  public String getAccountId(AvroFlumeEvent event) {
    for (Map.Entry<CharSequence, CharSequence> headerEntry :
      event.getHeaders().entrySet()) {
      String headerKey = headerEntry.getKey().toString();
      if (headerKey.equals(Constants.Gateway.CONTINUUITY_API_KEY)) {
        String apiKey = headerEntry.getValue().toString();
        return this.passportClient.getAccount(apiKey).getAccountId();
      }
    }
    // Key not found in headers
    throw new RuntimeException("event was not authenticated");
  }

  /**
   * Authenticates that the specified apiKey can access this cluster.
   *
   * @param apiKey for the request.
   * @return true
   */
  private boolean authenticate(String apiKey) {
    try {
      List<String> authorizedClusters =
        this.passportClient.getVPCList(apiKey);
      if (authorizedClusters == null || authorizedClusters.isEmpty()) {
        return false;
      }
      for (String authorizedCluster : authorizedClusters) {
        if (clusterName.equals(authorizedCluster)) {
          return true;
        }
      }
      LOG.trace("Failed to authenticate request using key: " + apiKey);
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

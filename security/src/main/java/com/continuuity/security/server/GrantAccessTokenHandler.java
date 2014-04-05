package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.security.Constants;
import com.continuuity.security.auth.AccessToken;
import com.continuuity.security.auth.AccessTokenIdentifier;
import com.continuuity.security.auth.Codec;
import com.continuuity.security.auth.TokenManager;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.apache.commons.codec.binary.Base64;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Generate and grant access token to authorized users.
 */
public class GrantAccessTokenHandler extends AbstractHandler {
  private TokenManager tokenManager;
  private final Codec<AccessTokenIdentifier> identifierCodec;
  private final CConfiguration configuration;
  private String[] roles;

  @Inject
  public GrantAccessTokenHandler(CConfiguration configuration ,
                                 TokenManager tokenManager,
                                 Codec<AccessTokenIdentifier> identifierCodec) {
    this.tokenManager = tokenManager;
    this.identifierCodec = identifierCodec;
    this.configuration = configuration;
    this.roles = new String[]{"user", "admin", "moderator"};
  }

  @Override
  public void handle(String s, HttpServletRequest request, HttpServletResponse response, int dispatch)
    throws IOException, ServletException {

    String username = request.getUserPrincipal().getName();
    Collection<String> userRoles = new ArrayList<String>();
    for (String role: roles) {
      if (request.isUserInRole(role)) {
        userRoles.add(role);
      }
    }
    long issueTime = System.currentTimeMillis();
    long tokenValidity = configuration.getInt(Constants.TOKEN_EXPIRATION, Constants.DEFAULT_TOKEN_EXPIRATION);
    long expireTime = issueTime + tokenValidity;
    // Create and sign a new AccessTokenIdentifier to generate the AccessToken.
    AccessTokenIdentifier tokenIdentifier = new AccessTokenIdentifier(username, userRoles, issueTime, expireTime);
    AccessToken token = tokenManager.signIdentifier(tokenIdentifier);

    // Set response headers
    response.setContentType("application/json;charset=UTF-8");
    response.addHeader("Cache-Control", "no-store");
    response.addHeader("Pragma", "no-cache");

    // Set response body
    AccessTokenIdentifier identifier = token.getIdentifier();
    long tokenExpiration = configuration.getInt(Constants.TOKEN_EXPIRATION, Constants.DEFAULT_TOKEN_EXPIRATION) / 1000;
    JsonObject json = new JsonObject();
    json.addProperty("access_token", new String(Base64.encodeBase64(identifierCodec.encode(identifier))));
    json.addProperty("token_type", "Bearer");
    json.addProperty("expires_in", tokenExpiration);

    response.getOutputStream().print(json.toString());
    response.setStatus(HttpServletResponse.SC_OK);
    ((Request) request).setHandled(true);
  }
}

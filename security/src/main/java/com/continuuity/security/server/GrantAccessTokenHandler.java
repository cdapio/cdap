package com.continuuity.security.server;

import com.continuuity.common.conf.Constants;
import com.continuuity.security.auth.AccessToken;
import com.continuuity.security.auth.AccessTokenIdentifier;
import com.continuuity.security.auth.Codec;
import com.continuuity.security.auth.TokenManager;
import com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.name.Named;
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
  private final TokenManager tokenManager;
  private final Codec<AccessToken> tokenCodec;
  private final long tokenValidity;

  @Inject
  public GrantAccessTokenHandler(TokenManager tokenManager,
                                 Codec<AccessToken> tokenCodec,
                                 @Named("token.validity.ms") long tokenValidity) {
    this.tokenManager = tokenManager;
    this.tokenCodec = tokenCodec;
    this.tokenValidity = tokenValidity;
  }

  @Override
  public void handle(String s, HttpServletRequest request, HttpServletResponse response, int dispatch)
    throws IOException, ServletException {

    String[] roles = Constants.Security.BASIC_USER_ROLES;
    String username = request.getUserPrincipal().getName();
    Collection<String> userRoles = new ArrayList<String>();
    for (String role: roles) {
      if (request.isUserInRole(role)) {
        userRoles.add(role);
      }
    }
    long issueTime = System.currentTimeMillis();
    long expireTime = issueTime + tokenValidity;
    // Create and sign a new AccessTokenIdentifier to generate the AccessToken.
    AccessTokenIdentifier tokenIdentifier = new AccessTokenIdentifier(username, userRoles, issueTime, expireTime);
    AccessToken token = tokenManager.signIdentifier(tokenIdentifier);

    // Set response headers
    response.setContentType("application/json;charset=UTF-8");
    response.addHeader("Cache-Control", "no-store");
    response.addHeader("Pragma", "no-cache");

    // Set response body
    JsonObject json = new JsonObject();
    byte[] encodedIdentifier = Base64.encodeBase64(tokenCodec.encode(token));
    json.addProperty("access_token", new String(encodedIdentifier, Charsets.UTF_8));
    json.addProperty("token_type", "Bearer");
    json.addProperty("expires_in", tokenValidity / 1000);

    response.getOutputStream().print(json.toString());
    response.setStatus(HttpServletResponse.SC_OK);
    ((Request) request).setHandled(true);
  }
}

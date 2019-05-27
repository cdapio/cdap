/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.security.server;

import org.apache.knox.gateway.services.security.token.impl.JWT;
import org.apache.knox.gateway.services.security.token.impl.JWTToken;
import org.apache.knox.gateway.util.CertificateUtils;
import org.apache.shiro.authz.UnauthorizedException;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.security.auth.AccessToken;
import co.cask.cdap.security.auth.AccessTokenIdentifier;
import co.cask.cdap.security.auth.TokenManager;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;

import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.interfaces.RSAPublicKey;
import java.util.Date;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

/**
 * Generate and grant access token to authorized users.
 */
@Path("/")
public class GrantAccessToken {
  private static final Logger LOG = LoggerFactory.getLogger(GrantAccessToken.class);
  private final TokenManager tokenManager;
  private final Codec<AccessToken> tokenCodec;
  private final long tokenExpiration;
  private final long extendedTokenExpiration;
  private static CConfiguration conf;

  /**
   * Create a new GrantAccessToken object to generate tokens for authorized users.
   */
  @Inject
  public GrantAccessToken(TokenManager tokenManager,
                          Codec<AccessToken> tokenCodec,
                          CConfiguration cConf) {
    this.tokenManager = tokenManager;
    this.tokenCodec = tokenCodec;
    this.conf = cConf;
    this.tokenExpiration = cConf.getLong(Constants.Security.TOKEN_EXPIRATION);
    this.extendedTokenExpiration = cConf.getLong(Constants.Security.EXTENDED_TOKEN_EXPIRATION);
  }

  /**
   * Initialize the TokenManager.
   */
  public void init() {
    tokenManager.start();
  }

  /**
   * Stop the TokenManager.
   */
  public void destroy() {
    tokenManager.stop();
  }

  /**
   * Paths to get Access Tokens.
   */
  public static final class Paths {
    public static final String GET_TOKEN = "token";
    public static final String GET_TOKEN_FROM_KNOX = "knoxToken";
    public static final String GET_EXTENDED_TOKEN = "extendedtoken";
  }

  /**
   *  Get an AccessToken from KNOXToken.
   */
  @Path(Paths.GET_TOKEN_FROM_KNOX)
  @GET
  @Produces("application/json")
  public Response tokenFromKNOX(@Context HttpServletRequest request, @Context HttpServletResponse response)
      throws IOException, ServletException {
    AccessToken token = getTokenFromKNOX(request, response);
    if (token != null) {
      setResponse(request, response, token, tokenExpiration);
      return Response.status(200).build();
    } else {
      return Response.status(201).build();
    }
  }  

  /**
   * Get an AccessToken.
   */
  @Path(Paths.GET_TOKEN)
  @GET
  @Produces("application/json")
  public Response token(@Context HttpServletRequest request, @Context HttpServletResponse response)
      throws IOException, ServletException {
    grantToken(request, response, tokenExpiration);
    return Response.status(200).build();
  }

  /**
   * Get a long lasting Access Token.
   */
  @Path(Paths.GET_EXTENDED_TOKEN)
  @GET
  @Produces("application/json")
  public Response extendedToken(@Context HttpServletRequest request, @Context HttpServletResponse response)
    throws IOException, ServletException {
    grantToken(request, response, extendedTokenExpiration);
    return Response.status(200).build();
  }

  private void setResponse(HttpServletRequest request, HttpServletResponse response, AccessToken token,
      long tokenValidity) throws IOException, ServletException {
    JsonObject json = new JsonObject();
    byte[] encodedIdentifier = Base64.encodeBase64(tokenCodec.encode(token));
    json.addProperty(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN,
        new String(encodedIdentifier, Charsets.UTF_8));
    json.addProperty(ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE,
        ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE_BODY);
    json.addProperty(ExternalAuthenticationServer.ResponseFields.EXPIRES_IN,
        TimeUnit.SECONDS.convert(tokenValidity, TimeUnit.MILLISECONDS));

    response.getOutputStream().print(json.toString());
    response.setStatus(HttpServletResponse.SC_OK);
  }

  private AccessToken getTokenFromKNOX(HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		
		final String authorizationHeader = request.getHeader("knoxToken");
        	String wireToken = null;
        	long expireTime = -1l;
        	String username = null;
        	long issueTime = System.currentTimeMillis();

        	if (authorizationHeader!=null && !Strings.isNullOrEmpty(authorizationHeader)) {
            		wireToken = authorizationHeader;
        	} else {
            		wireToken = getJWTTokenFromCookie(request);
        	}
        	if (Strings.isNullOrEmpty(wireToken)) {
        		LOG.debug("No valid 'Bearer Authorization' or 'Cookie' found in header, send 401");
            		return null;
        	}

        	JWTToken token;
        	try {
            		token = new JWTToken(wireToken);
            		username = token.getSubject();
        	} catch (ParseException | NullPointerException e) {
            		e.printStackTrace();
            		throw new UnauthorizedException("Authorization header missing/invalid");
        	}

        	/*boolean validToken = verifyToken(token);
        	if(!validToken)
            		throw new UnauthorizedException("Not authorized");*/
        	
        	Date expires = token.getExpiresDate();
        	LOG.debug("token expiry date: " + expires.toString());
        	if (expires.before(new Date()))
            		throw new UnauthorizedException("Token expired.");
        
        	expireTime = Date.parse(expires.toString());
        
		List<String> userGroups = Collections.emptyList();

		AccessTokenIdentifier tokenIdentifier = new AccessTokenIdentifier(username, userGroups, issueTime, expireTime);
		AccessToken accessToken = tokenManager.signIdentifier(tokenIdentifier);
		LOG.debug("Issued token for user {}", username);
		return accessToken;
	} 
 
  private static boolean verifyToken(JWT token) {
        boolean rc = false;
        String verificationPem = conf.get(Constants.Security.KNOX_TOKEN_PUBLIC_KEY);
        LOG.info("key value : " + verificationPem);
        try {
            RSAPublicKey publicKey = CertificateUtils.parseRSAPublicKey(verificationPem);
            JWSVerifier verifier = new RSASSAVerifier(publicKey);
            rc = token.verify(verifier);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                e.printStackTrace();
            }
            LOG.warn("Exception in verifying signature : ", e.toString());
            e.printStackTrace();
            return false;
        }
        return rc;
    }
	
	private static String getJWTTokenFromCookie(HttpServletRequest request) {
        String rawCookie = request.getHeader("cookie");
        if (rawCookie == null) {
        	return null;
        }
        String cookieToken = null;
        String cookieName = "hadoop-jwt";

        String[] rawCookieParams = rawCookie.split(";");
        for(String rawCookieNameAndValue :rawCookieParams) {
            String[] rawCookieNameAndValuePair = rawCookieNameAndValue.split("=");
            if ((rawCookieNameAndValuePair.length > 1) &&
                    (rawCookieNameAndValuePair[0].trim().equalsIgnoreCase(cookieName))) {
                cookieToken = rawCookieNameAndValuePair[1];
                break;
            }
        }
        return cookieToken;
    } 


  private void grantToken(HttpServletRequest request, HttpServletResponse response, long tokenValidity)
    throws IOException, ServletException {

    String username = request.getUserPrincipal().getName();
    List<String> userGroups = Collections.emptyList();

    long issueTime = System.currentTimeMillis();
    long expireTime = issueTime + tokenValidity;
    // Create and sign a new AccessTokenIdentifier to generate the AccessToken.
    AccessTokenIdentifier tokenIdentifier = new AccessTokenIdentifier(username, userGroups, issueTime, expireTime);
    AccessToken token = tokenManager.signIdentifier(tokenIdentifier);
    LOG.debug("Issued token for user {}", username);

    // Set response headers
    response.setContentType("application/json;charset=UTF-8");
    response.addHeader(HttpHeaderNames.CACHE_CONTROL.toString(), "no-store");
    response.addHeader(HttpHeaderNames.PRAGMA.toString(), "no-cache");

    // Set response body
    JsonObject json = new JsonObject();
    byte[] encodedIdentifier = Base64.encodeBase64(tokenCodec.encode(token));
    json.addProperty(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN,
                     new String(encodedIdentifier, Charsets.UTF_8));
    json.addProperty(ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE,
                     ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE_BODY);
    json.addProperty(ExternalAuthenticationServer.ResponseFields.EXPIRES_IN,
                     TimeUnit.SECONDS.convert(tokenValidity, TimeUnit.MILLISECONDS));

    response.getOutputStream().print(json.toString());
    response.setStatus(HttpServletResponse.SC_OK);
  }
}

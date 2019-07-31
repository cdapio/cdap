/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.security.authentication.DeferredAuthentication;

import java.io.IOException;
import java.net.URI;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.knox.gateway.services.security.token.impl.JWT;
import org.apache.knox.gateway.services.security.token.impl.JWTToken;
import org.apache.knox.gateway.util.CertificateUtils;
import org.apache.shiro.authz.UnauthorizedException;
import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Authentication.User;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.B64Code;
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.log.Log;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;

import co.cask.cdap.common.conf.Constants;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;

/**
 * @version $Rev: 4793 $ $Date: 2009-03-19 00:00:01 +0100 (Thu, 19 Mar 2009) $
 */
public class KnoxBasicAuthenticator extends BasicAuthenticator 
{   
	
	private Map<String, String> handlerProps;
	
    /* ------------------------------------------------------------ */
    public KnoxBasicAuthenticator(Map<String, String> handlerProps)
    {
    	this.handlerProps = handlerProps;
    }
    
    /* ------------------------------------------------------------ */
    /**
     * @see org.eclipse.jetty.security.Authenticator#getAuthMethod()
     */
    public String getAuthMethod()
    {
        return Constraint.__BASIC_AUTH;
    }

 

    /* ------------------------------------------------------------ */
    /**
     * @see org.eclipse.jetty.security.Authenticator#validateRequest(javax.servlet.ServletRequest, javax.servlet.ServletResponse, boolean)
     */
    public Authentication validateRequest(ServletRequest req, ServletResponse res, boolean mandatory) throws ServerAuthException
    {
        HttpServletRequest request = (HttpServletRequest)req;
        HttpServletResponse response = (HttpServletResponse)res;
        
        Log.info("handlerProps : " + handlerProps); 
        final String authorizationHeader = request.getHeader("Authorization");
        String wireToken = null;
        String username = null;

        //Getting either through knox UI or through cookie set 
        //but getting knox token itself.
        if (authorizationHeader!=null && !Strings.isNullOrEmpty(authorizationHeader) && (authorizationHeader.trim().toLowerCase().startsWith("bearer "))) {
            wireToken = authorizationHeader.substring(7);
        } else {
            wireToken = getJWTTokenFromCookie(request);
        }
        
        //Getting from Metaservice
        if (Strings.isNullOrEmpty(wireToken)) {
        	wireToken = request.getHeader("knoxToken");
        	Log.info("knox token : " + wireToken);
        }
        
        if (!Strings.isNullOrEmpty(wireToken)) {
	        JWTToken token;
	        try {
	            token = new JWTToken(wireToken);
	            username = token.getSubject();
	        } catch (ParseException | NullPointerException e) {
	            e.printStackTrace();
	            throw new UnauthorizedException("Authorization header missing/invalid");
	        }
	
	        boolean validToken = verifyToken(token);
	        if(!validToken)
	            throw new UnauthorizedException("Not authorized");
	
	        Date expires = token.getExpiresDate();
	        Log.debug("token expiry date: " + expires.toString());
	        if (expires != null && expires.before(new Date()))
	            throw new UnauthorizedException("Token expired.");
	        
	        UserIdentity user = login (username, null, request);
	        if (user!=null)
	        {
	            return new UserAuthentication(getAuthMethod(),user);
	        }
        }
        
        
        String credentials = request.getHeader(HttpHeaders.AUTHORIZATION);
        
        try
        {
            if (!mandatory)
                return new DeferredAuthentication(this);

            if (credentials != null)
            {                 
                int space=credentials.indexOf(' ');
                if (space>0)
                {
                    String method=credentials.substring(0,space);
                    if ("basic".equalsIgnoreCase(method))
                    {
                        credentials = credentials.substring(space+1);
                        credentials = B64Code.decode(credentials,StringUtil.__ISO_8859_1);
                        int i = credentials.indexOf(':');
                        if (i>0)
                        {
                            username = credentials.substring(0,i);
                            String password = credentials.substring(i+1);
                            
                            String username_password = username + ":" + password;
                            String encryptedCredentials = Base64.getEncoder().encodeToString(username_password.getBytes());
                            
                            String host = handlerProps.get(Constants.Security.KNOX_HOST);
			    Log.info("host : " + host);
                            String port = handlerProps.get(Constants.Security.KNOX_PORT);
			    Log.info("port : " + port);
                            String strAuthURI = "https://" + host + ":" + port + "/gateway/knoxsso/knoxtoken/api/v1/token";;
                    	    URI authURI = URI.create(strAuthURI);
                    	    
                    	    HttpRequest knoxRequest = HttpRequest.get(authURI.toURL()).addHeaders(getAuthenticationHeaders(encryptedCredentials)).build();
                    	    HttpResponse knoxResponse = HttpRequests.execute(knoxRequest, getHttpRequestConfig());
                    	    Log.info("knoxResponse.getResponseCode() : " + knoxResponse.getResponseCode());	
                    		if (knoxResponse.getResponseCode() == 200) {
	                            UserIdentity user = login (username, null, request);
	                            if (user!=null)
	                            {
	                                return new UserAuthentication(getAuthMethod(),user);
	                            }
                    		}
                        }
                    }
                }
            }
            
            if (DeferredAuthentication.isDeferred(response))
                return Authentication.UNAUTHENTICATED;
            
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "basic realm=\"" + _loginService.getName() + '"');
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return Authentication.SEND_CONTINUE;
        }
        catch (IOException e)
        {
            throw new ServerAuthException(e);
        }
    }

    public boolean secureResponse(ServletRequest req, ServletResponse res, boolean mandatory, User validatedUser) throws ServerAuthException
    {
        return true;
    }
    
    private boolean verifyToken(JWT token) {
        boolean rc = false;
        String verificationPem = handlerProps.get(Constants.Security.KNOX_TOKEN_PUBLIC_KEY);
        try {
            RSAPublicKey publicKey = CertificateUtils.parseRSAPublicKey(verificationPem);
            JWSVerifier verifier = new RSASSAVerifier(publicKey);
            rc = token.verify(verifier);
        } catch (Exception e) {
            if (Log.isDebugEnabled()) {
                e.printStackTrace();
            }
            Log.warn("Exception in verifying signature : ", e.toString());
            e.printStackTrace();
            return false;
        }
        return rc;
    }
    
    private String getJWTTokenFromCookie(HttpServletRequest request) {
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
    
    private Multimap<String, String> getAuthenticationHeaders(String accessToken) {
	    return ImmutableMultimap.of("authorization", "Basic " + accessToken);
	  }
    
    private HttpRequestConfig getHttpRequestConfig() {
	    return new HttpRequestConfig(0, 0, false);
	}

}


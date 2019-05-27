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
import java.text.ParseException;
import java.util.Date;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.knox.gateway.services.security.token.impl.JWTToken;
import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.security.ServerAuthException;
import org.eclipse.jetty.security.UserAuthentication;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Authentication.User;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.B64Code;
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.security.Constraint;
import org.mortbay.log.Log;

/**
 *  * @version $Rev: 4793 $ $Date: 2009-03-19 00:00:01 +0100 (Thu, 19 Mar 2009) $
 *   */
public class KnoxBasicAuthenticator extends BasicAuthenticator 
{   
    /* ------------------------------------------------------------ */
    public KnoxBasicAuthenticator()
    {
    }
    
    /* ------------------------------------------------------------ */
    /**
 *      * @see org.eclipse.jetty.security.Authenticator#getAuthMethod()
 *           */
    public String getAuthMethod()
    {
        return Constraint.__BASIC_AUTH;
    }

 

    /* ------------------------------------------------------------ */
    /**
 *      * @see org.eclipse.jetty.security.Authenticator#validateRequest(javax.servlet.ServletRequest, javax.servlet.ServletResponse, boolean)
 *           */
    public Authentication validateRequest(ServletRequest req, ServletResponse res, boolean mandatory) throws ServerAuthException
    {
        HttpServletRequest request = (HttpServletRequest)req;
        HttpServletResponse response = (HttpServletResponse)res;
        //String credentials = request.getHeader(HttpHeaders.AUTHORIZATION);
        String knoxToken = request.getHeader("knoxToken");
        Log.info("knox token : " + knoxToken);
        //Log.info("credentials : " + credentials);

        JWTToken token = null;
		try {
			token = new JWTToken(knoxToken);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
        Log.info("JWT token : " + token);
        Log.info("expiry : " + token.getExpiresDate().toString());
        Log.info("username : " + token.getSubject());
        long expireTime = Date.parse(token.getExpiresDate().toString());
        String username_knox = token.getSubject();

        try
        {
            if (!mandatory)
                return new DeferredAuthentication(this);

           /* if (credentials != null)
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
                            String username = credentials.substring(0,i);
                            String password = credentials.substring(i+1);
                            
                            Log.info("username : " + username);
                            Log.info("password : " + password);

                            UserIdentity user = login (username, password, request);
                            if (user!=null)
                            {
                                return new UserAuthentication(getAuthMethod(),user);
                            }
                        }
                    }
                }
            }*/

            UserIdentity user = login (username_knox, null, request);
            if (user!=null)
            {
                return new UserAuthentication(getAuthMethod(),user);
            }

            
            if (DeferredAuthentication.isDeferred(response))
                return Authentication.UNAUTHENTICATED;
            
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "basic realm=\"" + _loginService.getName() + '"');
            response.setHeader("knoxToken", knoxToken);
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

}



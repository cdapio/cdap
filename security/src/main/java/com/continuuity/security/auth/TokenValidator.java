package com.continuuity.security.auth;

/**
 * Created by prupakheti on 4/7/14.
 */
import com.continuuity.gateway.util.Util;

import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.io.IOException;

/**
 * Created by prupakheti on 4/4/14.
 */
public class TokenValidator {
  private TokenManager tokenManager;
  private Codec<AccessToken> accessTokenCodec;
  private String encoding;

  private String errorHTTPResponse;
  private HttpResponse httpResponse;

  @Inject
  public TokenValidator(TokenManager tokenManager, Codec<AccessToken> accessTokenCodec) {
    this.tokenManager = tokenManager;
    this.accessTokenCodec = accessTokenCodec;
    this.encoding = "base64";
    this.errorHTTPResponse = null;
    this.httpResponse = null;
  }

  public TokenValidator(){

  }
  public boolean validate(String token) throws IOException {
    boolean flag = true;
    this.errorHTTPResponse = null;
    if (token == null) {
      flag = false;
      errorHTTPResponse = "HTTP/1.1 401 Unauthorized\n" + "WWW-Authenticate: Bearer realm=\"example\"";
      httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
      httpResponse.addHeader("WWW-Authenticate","Bearer realm = example");
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
      return flag;
    }
    byte[] decodedToken = Util.decodeBinary(token, encoding);
    AccessToken accessToken = accessTokenCodec.decode(decodedToken);
    try {
      tokenManager.validateSecret(accessToken);
    } catch (InvalidTokenException ite) {
      flag = false;
      errorHTTPResponse = "HTTP/1.1 401 Unauthorized\n" +
        "WWW-Authenticate: Bearer realm=\"example\",\n" +
        "                  error=\"invalid_token\",\n" +
        "                  error_description=\"The access token expired\"";
      httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
      httpResponse.addHeader("WWW-Authenticate","Bearer realm = example");

    }

    return flag;
  }

  public String getErrorHTTPResponse() {
    return errorHTTPResponse;
  }

  public HttpResponse getHttpResponse(){
    return httpResponse;
  }

}


/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.filters;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Implements Security filter to filter out requests from unknown sources.
 */
public class ContinuuitySecurityFilter implements Filter {

  private static final String CONTINUUITY_SIGNATURE = "abcdef";
  private static final String CONTINUUITY_SIGNATURE_HEADER = "X-Continuuity-Signature";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }


  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
    ServletException {
    HttpServletRequest req = (HttpServletRequest) request;
    String continuuitySignature = req.getHeader(CONTINUUITY_SIGNATURE_HEADER);
    if (validContinuuitySignature(continuuitySignature)) {
      chain.doFilter(request, response);
    } else {
      HttpServletResponse httpResponse = (HttpServletResponse) response;
      httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
    }
  }

  @Override
  public void destroy() {
  }

  private boolean validContinuuitySignature(String signature) {
    if (signature != null && !signature.isEmpty()) {
      return CONTINUUITY_SIGNATURE.equals(signature.trim());
    } else {
      return false;
    }
  }


}

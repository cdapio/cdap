package com.continuuity.performance.gateway;

/**
 * HttpPoster.
 */
public interface HttpPoster {
  void post(byte[] message) throws Exception;
  void post(String message) throws Exception;
}


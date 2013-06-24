package com.continuuity.performance.gateway;

/**
 * HttpClient interface for sending events to Gateway.
 */
public interface HttpClient {
  void post(byte[] message) throws Exception;
}


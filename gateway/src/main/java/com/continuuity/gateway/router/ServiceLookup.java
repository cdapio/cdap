package com.continuuity.gateway.router;

/**
 * Interface to lookup services that the router forwards.
 */
public interface ServiceLookup {
  String getService(int port);
}

package com.continuuity.discovery;

import java.net.InetSocketAddress;

/**
 *
 */
public interface Discoverable {

  /**
   * @return Name of the service
   */
  String getName();

  /**
   * @return An {@link InetSocketAddress} representing the host+port of the service.
   */
  InetSocketAddress getSocketAddress();

  //TODO: More methods, like getConfig() etc.
}

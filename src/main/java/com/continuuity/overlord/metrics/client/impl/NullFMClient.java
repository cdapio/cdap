package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.FMClient;

/**
 *
 *
 */
public class NullFMClient implements FMClient {
  
  @Override
  public boolean registerFlow(String name, String instance, String definition) {
    return true;
  }
  
  @Override
  public boolean registerStart(String name, String instance) {
    return true;
  }
  
  @Override
  public boolean registerStop(String name, String instance) {
    return true;
  }

}

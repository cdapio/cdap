package com.continuuity.overlord.metrics.client;

/**
 *
 *
 */
public interface FMClient {
  public boolean registerFlow(String name, String instance, String definition);
  public boolean registerStart(String name, String instance);
  public boolean registerStop(String name, String instance);
}

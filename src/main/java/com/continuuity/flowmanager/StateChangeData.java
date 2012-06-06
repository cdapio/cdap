package com.continuuity.flowmanager;

/**
 *
 */
public interface StateChangeData {
  public long getTimestamp();
  public String getAccountId();
  public String getApplication();
  public String getFlowName();
  public String getPayload();
  public StateChangeType getType();
}

package com.continuuity.overlord.flowmanager.internal;

import com.continuuity.overlord.flowmanager.StateChangeData;
import com.continuuity.overlord.flowmanager.StateChangeType;
import com.google.common.base.Objects;

/**
 *
 */
final class StateChangeDataImpl implements StateChangeData {
  private final long timestamp;
  private final String accountId;
  private final String application;
  private final String flowname;
  private final String payload;
  private final StateChangeType type;

  public StateChangeDataImpl(long timestamp, String accountId, String application, String flowname,
                             String payload, StateChangeType type) {
    this.timestamp = timestamp;
    this.accountId = accountId;
    this.application = application;
    this.flowname = flowname;
    this.payload = payload;
    this.type = type;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getAccountId() {
    return accountId;
  }

  @Override
  public String getApplication() {
    return application;
  }

  @Override
  public String getFlowName() {
    return flowname;
  }

  @Override
  public String getPayload() {
    return payload;
  }

  @Override
  public StateChangeType getType() {
    return type;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("timestamp", timestamp)
      .add("accountid", accountId)
      .add("application", application)
      .add("flowname", flowname)
      .add("payload", payload)
      .add("type", type)
      .toString();
  }
}

package com.continuuity.observer.internal;

import com.continuuity.observer.StateChangeData;
import com.continuuity.observer.StateChangeType;
import com.google.common.base.Objects;

/**
 *
 */
final class StateChangeDataImpl implements StateChangeData {
  private final long timestamp;
  private final String accountId;
  private final String appId;
  private final String flowId;
  private final String runId;
  private final String payload;
  private final StateChangeType type;

  public StateChangeDataImpl(long timestamp, String accountId, String appId, String flowId, String runId,
                             String payload, StateChangeType type) {
    this.timestamp = timestamp;
    this.accountId = accountId;
    this.appId = appId;
    this.runId = runId;
    this.flowId = flowId;
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
    return appId;
  }

  @Override
  public String getRunId() {
    return runId  ;
  }

  @Override
  public String getFlowName() {
    return flowId;
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
      .add("application", appId)
      .add("flowname", flowId)
      .add("runid", runId)
      .add("payload", payload)
      .add("type", type)
      .toString();
  }
}

package com.continuuity.common.logger;

/**
 * Log Tag that specifies the tag for a flow log.
 */
public class FlowLogTag implements LogTag {
  private String tag;

  public FlowLogTag(String accountId, String appId, String flowId,
                    int instanceId, String runId) {
    this.tag =
      String.format("%s.%s.%s.%s.%d", accountId, appId, runId, flowId,
                    instanceId );
  }

  @Override
  public String getTag() {
    return tag;
  }
}

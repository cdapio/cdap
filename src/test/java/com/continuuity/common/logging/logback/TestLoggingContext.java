package com.continuuity.common.logging.logback;

import com.continuuity.common.logging.ApplicationLoggingContext;

public class TestLoggingContext extends ApplicationLoggingContext {
  public TestLoggingContext(String accountId, String applicationId) {
    super(accountId, applicationId);
  }

  @Override
  public String getLogPartition() {
    return super.getLogPartition();
  }
}

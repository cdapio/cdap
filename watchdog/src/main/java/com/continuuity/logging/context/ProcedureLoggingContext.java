/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.context;

import com.continuuity.common.logging.ApplicationLoggingContext;

/**
 *
 */
public class ProcedureLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_PROCEDURE_ID = ".procedureId";

  /**
   * Constructs the ProcedureLoggingContext.
   * @param accountId account id
   * @param applicationId application id
   * @param procedureId flow id
   */
  public ProcedureLoggingContext(final String accountId,
                               final String applicationId,
                               final String procedureId) {
    super(accountId, applicationId);
    setSystemTag(TAG_PROCEDURE_ID, procedureId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_PROCEDURE_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/procedure-%s", super.getLogPathFragment(), getSystemTag(TAG_PROCEDURE_ID));
  }
}

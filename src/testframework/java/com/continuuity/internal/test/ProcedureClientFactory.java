package com.continuuity.internal.test;

import com.continuuity.test.ProcedureClient;
import com.google.inject.assistedinject.Assisted;

/**
 *
 */
public interface ProcedureClientFactory {

  ProcedureClient create(@Assisted("accountId") String accountId,
                         @Assisted("applicationId") String applicationId,
                         @Assisted("procedureName") String procedureName);
}

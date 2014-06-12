package com.continuuity.test.internal;

import com.continuuity.test.ProcedureClient;
import com.google.inject.assistedinject.Assisted;

/**
 * This interface is using Guice assisted inject to create instance of {@link com.continuuity.test.ProcedureClient}.
 */
public interface ProcedureClientFactory {

  ProcedureClient create(@Assisted("accountId") String accountId, @Assisted("applicationId") String applicationId,
                         @Assisted("procedureName") String procedureName);
}

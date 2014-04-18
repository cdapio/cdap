/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.application;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.filesystem.Location;

/**
 *
 */
public interface BenchmarkManagerFactory {

  DefaultBenchmarkManager create(AuthToken token,
                                 @Assisted("accountId") String accountId,
                                 @Assisted("applicationId") String applicationId,
                                 AppFabricService.Iface appFabricServer,
                                 Location deployedJar,
                                 ApplicationSpecification appSpec);
}

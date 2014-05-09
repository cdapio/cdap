/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.application;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.gateway.handlers.AppFabricHttpHandler;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.filesystem.Location;

/**
 *
 */
public interface BenchmarkManagerFactory {

  DefaultBenchmarkManager create(@Assisted("accountId") String accountId,
                                 @Assisted("applicationId") String applicationId,
                                 AppFabricHttpHandler httpHandler,
                                 Location deployedJar,
                                 ApplicationSpecification appSpec);
}

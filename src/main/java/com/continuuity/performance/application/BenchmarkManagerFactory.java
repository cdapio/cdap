package com.continuuity.performance.application;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.weave.filesystem.Location;
import com.google.inject.assistedinject.Assisted;

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
